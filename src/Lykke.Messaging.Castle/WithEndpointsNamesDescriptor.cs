using System.Collections.Generic;
using Castle.Core;
using Castle.MicroKernel;
using Castle.MicroKernel.ModelBuilder;

namespace Lykke.Messaging.Castle
{
    internal class WithEndpointsNamesDescriptor : IComponentModelDescriptor
    {
        private readonly Dictionary<string, string> m_EndpointsNamingMapping;

        public WithEndpointsNamesDescriptor(Dictionary<string, string> endpointsNamingMapping)
        {
            m_EndpointsNamingMapping = endpointsNamingMapping;
        }

        public void BuildComponentModel(IKernel kernel, ComponentModel model)
        {
        }

        public void ConfigureComponentModel(IKernel kernel, ComponentModel model)
        {
            IDictionary<string, string> endpointNames = new Dictionary<string, string>(m_EndpointsNamingMapping);
            if (model.ExtendedProperties.Contains("endpointNames"))
            {
                var oldEndpointsMapping = (IDictionary<string, string>)model.ExtendedProperties["endpointNames"];
                foreach (var p in oldEndpointsMapping)
                {
                    endpointNames[p.Key] = p.Value;
                }
            }

            model.ExtendedProperties["endpointNames"] = endpointNames; 
        }
    }
}