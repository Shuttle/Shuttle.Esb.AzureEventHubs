using Shuttle.Core.Contract;

namespace Shuttle.Esb.AzureEventHubs;

public class ConfigureEventArgs<T> where T : class
{
    private T _options;

    public ConfigureEventArgs(T options)
    {
        _options = Guard.AgainstNull(options);
    }

    public T Options
    {
        get => _options;
        set => _options = Guard.AgainstNull(value);
    }
}