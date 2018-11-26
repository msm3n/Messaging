namespace Lykke.Messaging.Contract
{
    public struct Destination
    {
        public string Publish { get; set; }
        public string Subscribe { get; set; }

        public static implicit operator Destination(string destination)
        {
            return new Destination
            {
                Publish = destination,
                Subscribe = destination
            };
        }

        public bool Equals(Destination other)
        {
            return string.Equals(Publish, other.Publish) && string.Equals(Subscribe, other.Subscribe);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is Destination && Equals((Destination) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Publish != null ? Publish.GetHashCode() : 0)*397) ^ (Subscribe != null ? Subscribe.GetHashCode() : 0);
            }
        }

        public static bool operator ==(Destination left, Destination right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(Destination left, Destination right)
        {
            return !left.Equals(right);
        }

        public override string ToString()
        {
            if (Subscribe == Publish)
                return "["+Subscribe+"]";
            return $"[s:{Subscribe}, p:{Publish}]";
        }
    }
}
