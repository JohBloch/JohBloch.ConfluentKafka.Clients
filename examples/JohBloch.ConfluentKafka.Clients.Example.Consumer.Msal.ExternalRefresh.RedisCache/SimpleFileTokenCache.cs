using Microsoft.Identity.Client;

namespace JohBloch.ConfluentKafka.Clients.Example.Consumer.Msal.ExternalRefresh.RedisCache;

internal static class SimpleFileTokenCache
{
    private static readonly SemaphoreSlim Gate = new(1, 1);

    public static void Bind(ITokenCache tokenCache, string cacheFilePath)
    {
        tokenCache.SetBeforeAccess(args =>
        {
            Gate.Wait();

            try
            {
                if (!File.Exists(cacheFilePath))
                {
                    return;
                }

                var data = File.ReadAllBytes(cacheFilePath);
                args.TokenCache.DeserializeMsalV3(data, shouldClearExistingCache: true);
            }
            finally
            {
                Gate.Release();
            }
        });

        tokenCache.SetAfterAccess(args =>
        {
            if (!args.HasStateChanged)
            {
                return;
            }

            Gate.Wait();

            try
            {
                var directory = Path.GetDirectoryName(cacheFilePath);
                if (!string.IsNullOrWhiteSpace(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                var data = args.TokenCache.SerializeMsalV3();
                File.WriteAllBytes(cacheFilePath, data);
            }
            finally
            {
                Gate.Release();
            }
        });
    }
}
