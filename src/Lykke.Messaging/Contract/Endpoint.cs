using Lykke.Messaging.Serialization;
using System;

namespace Lykke.Messaging.Contract
{
    /// <summary>
	/// Endpoint
	/// </summary>
	public struct Endpoint
	{
	    /// <summary>Gets or sets the transport id.</summary>
		/// <value>The transport id.</value>
		public string TransportId { get; set; }

	    /// <summary>Gets or sets the destination.</summary>
		/// <value>The destination.</value>
        public Destination Destination { get; set; }

	    /// <summary>Shared destination</summary>
		public bool SharedDestination { get; set; }

	    /// <summary>Shared destination</summary>
		public SerializationFormat SerializationFormat { get; set; }

	    /// <summary>
        /// 
        /// </summary>
        public Endpoint(
            string transportId,
            string destination,
            bool sharedDestination = false,
            SerializationFormat serializationFormat = SerializationFormat.ProtoBuf)
		{
            TransportId = transportId;
			Destination = destination ?? throw new ArgumentNullException(nameof(destination));
			SharedDestination = sharedDestination;
		    SerializationFormat = serializationFormat;
		}

        /// <summary>
		/// 
		/// </summary>
        public Endpoint(
            string transportId,
            string publish,
            string subscribe,
            bool sharedDestination = false,
            SerializationFormat serializationFormat = SerializationFormat.ProtoBuf)
		{
		    TransportId = transportId;
			Destination = new Destination {Publish = publish, Subscribe = subscribe};
			SharedDestination = sharedDestination;
		    SerializationFormat = serializationFormat;
		}

	    public override string ToString()
	    {
	        return $"[Transport: {TransportId}, Destination: {Destination}]";
	    }
	}
}
