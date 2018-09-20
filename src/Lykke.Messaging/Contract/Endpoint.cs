using System;

namespace Lykke.Messaging.Contract
{
    /// <summary>
	/// Endpoint
	/// </summary>
	public struct Endpoint
	{
        private string m_TransportId;
        private Destination m_Destination;
        private bool m_SharedDestination;
        private string m_SerializationFormat;

        /// <summary>Gets or sets the transport id.</summary>
		/// <value>The transport id.</value>
		public string TransportId
        {
            get { return m_TransportId; }
            set { m_TransportId = value; }
        }

        /// <summary>Gets or sets the destination.</summary>
		/// <value>The destination.</value>
        public Destination Destination
        {
            get { return m_Destination; }
            set { m_Destination = value; }
        }

        /// <summary>Shared destination</summary>
		public bool SharedDestination
        {
            get { return m_SharedDestination; }
            set { m_SharedDestination = value; }
        }

        /// <summary>Shared destination</summary>
		public string SerializationFormat
        {
            get { return m_SerializationFormat ?? "protobuf"; }
            set { m_SerializationFormat = value ?? "protobuf"; }
        }

        /// <summary>
        /// 
        /// </summary>
        public Endpoint(
            string transportId,
            string destination,
            bool sharedDestination = false,
            string serializationFormat = "protobuf")
		{
            m_TransportId = transportId;
			m_Destination = destination ?? throw new ArgumentNullException("destination");
			m_SharedDestination = sharedDestination;
		    m_SerializationFormat = serializationFormat;
		}

        /// <summary>
		/// 
		/// </summary>
        public Endpoint(
            string transportId,
            string publish,
            string subscribe,
            bool sharedDestination = false,
            string serializationFormat = "protobuf")
		{
		    m_TransportId = transportId;
			m_Destination = new Destination {Publish = publish, Subscribe = subscribe};
			m_SharedDestination = sharedDestination;
		    m_SerializationFormat = serializationFormat;
		}

	    public override string ToString()
	    {
	        return $"[Transport: {TransportId}, Destination: {Destination}]";
	    }
	}
}
