using Shuttle.Core.Contract;

namespace Shuttle.Esb.AzureEventHubs
{
    public class ConfigureEventArgs<T> where T : class
    {
        private T _options;

        public T Options
        {
            get => _options;
            set => _options = value ?? throw new System.ArgumentNullException();
        }

        public ConfigureEventArgs(T options)
        {
            Guard.AgainstNull(options, nameof(options));

            _options = options;
        }
    }
}