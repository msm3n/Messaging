using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Castle.Core;
using Castle.MicroKernel;
using Castle.MicroKernel.Context;
using Castle.MicroKernel.Facilities;
using Castle.MicroKernel.Registration;
using Common.Log;
using Lykke.Messaging.Configuration;
using Lykke.Messaging.Contract;

namespace Lykke.Messaging.Castle
{
    public class MessagingFacility : AbstractFacility
    {
        private readonly IDictionary<string, JailStrategy> m_JailStrategies = new Dictionary<string, JailStrategy>();
        private readonly List<IHandler> m_SerializerFactoryWaitList = new List<IHandler>();
        private readonly List<IHandler> m_MessageHandlerWaitList = new List<IHandler>();
        private readonly List<Action<IKernel>> m_InitPreSteps = new List<Action<IKernel>>();
        private readonly List<Action<IKernel>> m_InitPostSteps = new List<Action<IKernel>>();
        private readonly List<ITransportFactory> m_TransportFactories=new List<ITransportFactory>();
        private readonly MessagingConfiguration m_DefaultMessagingConfiguration=new MessagingConfiguration();
        private IMessagingEngine m_MessagingEngine;
        private bool m_IsExplicitConfigurationProvided = false;
        private IEndpointProvider m_EndpointProvider;

        private IMessagingConfiguration MessagingConfiguration { get; set; }

        public MessagingFacility()
        {
            MessagingConfiguration = m_DefaultMessagingConfiguration;
        }

        public MessagingFacility WithTransportFactory(ITransportFactory factory)
        {
            m_TransportFactories.Add(factory);
            return this;
        }

        public MessagingFacility WithTransportFactory<T>() where T : ITransportFactory, new()
        {
            m_TransportFactories.Add(Activator.CreateInstance<T>());
            return this;
        }

        public MessagingFacility WithTransport(string name, TransportInfo transport)
        {
            if (m_IsExplicitConfigurationProvided)
                throw new InvalidOperationException("Can not add transport to since configuration is provided explicitly");
            if (name == null) throw new ArgumentNullException("name");
            if (transport == null) throw new ArgumentNullException("transport");
            m_DefaultMessagingConfiguration.Transports.Add(name, transport);
            return this;
        }

        public MessagingFacility WithProcessingGroup(string name, ProcessingGroupInfo processingGroup)
        {
            if (m_IsExplicitConfigurationProvided)
                throw new InvalidOperationException("Can not add processing group to since configuration is provided explicitly");
            if (name == null) throw new ArgumentNullException("name");
            if (processingGroup == null) throw new ArgumentNullException("processingGroup");
            m_DefaultMessagingConfiguration.ProcessingGroups.Add(name, processingGroup);
            return this;
        }

        public MessagingFacility WithJailStrategy(string name, JailStrategy jailStrategy)
        {
            if (name == null) throw new ArgumentNullException("name");
            if (jailStrategy == null) throw new ArgumentNullException("jailStrategy");
            m_JailStrategies.Add(name, jailStrategy);
            return this;
        }

        public MessagingFacility WithConfiguration(IMessagingConfiguration configuration)
        {
            m_IsExplicitConfigurationProvided = true;
            MessagingConfiguration = configuration;
            return this;
        }

        public MessagingFacility WithConfigurationFromContainer()
        {
            m_IsExplicitConfigurationProvided = true;
            AddInitStep(kernel => WithConfiguration(kernel.Resolve<IMessagingConfiguration>()) );
            return this;
        }

        public MessagingFacility VerifyEndpoints(EndpointUsage usage,bool configureIfRequired,params string[] endpoints)
        {
            AddPostInitStep(kernel =>
                endpoints.Select(ep => m_EndpointProvider.Get(ep)).ToList().ForEach(endpoint =>
                {
                    string error;
                    if (!m_MessagingEngine.VerifyEndpoint(endpoint, usage, configureIfRequired, out error))
                        throw new ApplicationException(error);
                }));
            return this;
        }

        public void AddInitStep(Action<IKernel> step)
        {
            m_InitPreSteps.Add(step);
        }

        public void AddPostInitStep(Action<IKernel> step)
        {
            m_InitPostSteps.Add(step);
        }

        protected override void Dispose()
        {
            m_MessagingEngine.Dispose();
            base.Dispose();
        }

        protected override void Init()
        {
            foreach (var initStep in m_InitPreSteps)
            {
                initStep(Kernel);
            }

            if (Kernel.HasComponent(typeof (IEndpointProvider)))
                throw new Exception("IEndpointProvider already registered in container, can not register IEndpointProvider from MessagingConfiguration");

            Kernel.Register(
                Component.For<IEndpointProvider>()
                    .Forward<ISubDependencyResolver>()
                    .ImplementedBy<EndpointResolver>()
                    .Named("EndpointResolver")
                    .DependsOn(new { endpoints = MessagingConfiguration.GetEndpoints() }));
            var subDependencyResolver = Kernel.Resolve<ISubDependencyResolver>("EndpointResolver");
            m_EndpointProvider = Kernel.Resolve<IEndpointProvider>("EndpointResolver");
            Kernel.Resolver.AddSubResolver(subDependencyResolver);

            m_MessagingEngine = new MessagingEngine(new LogToConsole(), 
                new TransportResolver(MessagingConfiguration.GetTransports() ?? new Dictionary<string, TransportInfo>(), m_JailStrategies),
                MessagingConfiguration.GetProcessingGroups(),
                m_TransportFactories.ToArray());

            Kernel.Register(
                Component.For<IMessagingEngine>().Instance(m_MessagingEngine)
                );
            Kernel.ComponentRegistered += OnComponentRegistered;
            Kernel.ComponentModelCreated += ProcessModel;
            foreach (var initStep in m_InitPostSteps)
            {
                initStep(Kernel);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void OnComponentRegistered(string key, IHandler handler)
        {
            if ((bool)(handler.ComponentModel.ExtendedProperties["IsSerializerFactory"] ?? false))
            {
                m_SerializerFactoryWaitList.Add(handler);
            }

            var messageHandlerFor = handler.ComponentModel.ExtendedProperties["MessageHandlerFor"] as string[];
            if (messageHandlerFor != null && messageHandlerFor.Length > 0)
            {
                m_MessageHandlerWaitList.Add(handler);
            }

            ProcessWaitList();
        }

        private bool TryRegisterSerializerFactory(IHandler handler)
        {
            var factory = handler.TryResolve(CreationContext.CreateEmpty());
            if (factory == null)
                return false;
            m_MessagingEngine.SerializationManager.RegisterSerializerFactory(factory as ISerializerFactory);
            return true;
        }
 
        private void ProcessWaitList()
        {
            foreach (var handler in m_MessageHandlerWaitList.ToArray())
            {
                if (TryStart(handler))
                    m_MessageHandlerWaitList.Remove(handler);
            }

            foreach (var factoryHandler in m_SerializerFactoryWaitList.ToArray())
            {
                if(TryRegisterSerializerFactory(factoryHandler))
                    m_SerializerFactoryWaitList.Remove(factoryHandler);
            }
        }

        /// <summary>
        /// Request the component instance
        /// </summary>
        /// <param name="handler"/>
        private bool TryStart(IHandler handler)
        {
            return handler.TryResolve(CreationContext.CreateEmpty()) != null;
        }

        private void ProcessModel(ComponentModel model)
        {
            var messageHandlerFor = model.ExtendedProperties["MessageHandlerFor"] as string[];
            if (messageHandlerFor != null && messageHandlerFor.Length > 0)
            {
                model.CustomComponentActivator = typeof(MessageHandlerActivator);
            }

            if (model.Services.Contains(typeof(ISerializerFactory)))
            {
                model.ExtendedProperties["IsSerializerFactory"] = true;
            }
            else
            {
                model.ExtendedProperties["IsSerializerFactory"] = false;
            }
        }
    }
}