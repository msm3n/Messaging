using System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

public static class Log
{
    private static ILoggerFactory _factory;

    /// <summary>
    /// Вызывать на старте приложения _до_ создания любых объектов Lykke.Messaging
    /// </summary>
    public static void Configure(ILoggerFactory factory)
    {
        _factory = factory ?? throw new ArgumentNullException(nameof(factory));
    }

    /// <summary>
    /// Получить logger для T
    /// </summary>
    public static ILogger<T> For<T>()
    {
        // Если фабрика не настроена — возвращаем "no-op" логгер,
        // чтобы статические инициализаторы библиотечных классов не падали.
        if (_factory == null)
            return NullLogger<T>.Instance;

        return _factory.CreateLogger<T>();
    }
}