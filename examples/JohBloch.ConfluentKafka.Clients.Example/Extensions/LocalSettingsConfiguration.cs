using System.Text.Json;
using Microsoft.Extensions.Configuration;

internal static class LocalSettingsConfiguration
{
    public static IConfiguration Build()
    {
        string baseDir = AppContext.BaseDirectory;
        string samplePath = Path.Combine(baseDir, "local.settings.sample.json");
        string localPath = Path.Combine(baseDir, "local.settings.json");

        var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        LoadValuesFromLocalSettings(values, samplePath);
        LoadValuesFromLocalSettings(values, localPath);

        var envValues = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (System.Collections.DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            if (entry.Key is not string rawKey)
            {
                continue;
            }

            string? value = entry.Value?.ToString();
            if (value == null)
            {
                continue;
            }

            string key = rawKey.Replace("__", ":", StringComparison.Ordinal);
            envValues[key] = value;
        }

        return new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .AddInMemoryCollection(envValues)
            .Build();
    }

    private static void LoadValuesFromLocalSettings(Dictionary<string, string?> values, string path)
    {
        if (!File.Exists(path))
        {
            return;
        }

        try
        {
            string json = File.ReadAllText(path);
            using JsonDocument doc = JsonDocument.Parse(json);

            if (!doc.RootElement.TryGetProperty("Values", out JsonElement valuesElement)
                || valuesElement.ValueKind != JsonValueKind.Object)
            {
                return;
            }

            foreach (JsonProperty prop in valuesElement.EnumerateObject())
            {
                string key = prop.Name.Replace("__", ":", StringComparison.Ordinal);

                values[key] = prop.Value.ValueKind == JsonValueKind.String
                    ? prop.Value.GetString()
                    : prop.Value.ToString();
            }
        }
        catch (JsonException)
        {
            // Treat invalid local settings as "no settings".
        }
    }
}
