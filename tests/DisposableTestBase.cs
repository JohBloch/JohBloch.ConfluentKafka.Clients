using System;
using System.Collections.Generic;

namespace JohBloch.ConfluentKafka.Clients.Tests;

public abstract class DisposableTestBase : IDisposable
{
    private readonly List<IDisposable> _disposables = new();

    protected T Track<T>(T disposable) where T : IDisposable
    {
        _disposables.Add(disposable);
        return disposable;
    }

    protected void TrackDisposable(IDisposable disposable)
    {
        _disposables.Add(disposable);
    }

    public void Dispose()
    {
        for (int i = _disposables.Count - 1; i >= 0; i--)
        {
            try
            {
                _disposables[i]?.Dispose();
            }
            catch
            {
                // test cleanup should never fail the test
            }
        }

        _disposables.Clear();
    }
}
