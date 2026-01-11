using System.Reflection;
using System.Runtime.Loader;
using Microsoft.Build.Locator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.MSBuild;

static int Fail(string message)
{
    Console.Error.WriteLine(message);
    return 1;
}

string repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", ".."));
string solutionPath = Path.Combine(repoRoot, "JohBloch.ConfluentKafka.Clients.sln");
string projectName = "JohBloch.ConfluentKafka.Clients";
string configuration = "Release";

for (int i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--solution" when i + 1 < args.Length:
            solutionPath = Path.GetFullPath(args[++i]);
            break;
        case "--project" when i + 1 < args.Length:
            projectName = args[++i];
            break;
        case "--configuration" when i + 1 < args.Length:
            configuration = args[++i];
            break;
        default:
            return Fail($"Unknown or incomplete arg: '{args[i]}'");
    }
}

if (!File.Exists(solutionPath))
{
    return Fail($"Solution not found: {solutionPath}");
}

if (!MSBuildLocator.IsRegistered)
{
    MSBuildLocator.RegisterDefaults();
}

var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
    ["Configuration"] = configuration,
};

using var workspace = MSBuildWorkspace.Create(properties);
var solution = await workspace.OpenSolutionAsync(solutionPath);

if (workspace.Diagnostics.Any())
{
    foreach (var d in workspace.Diagnostics)
    {
        Console.Error.WriteLine(d);
    }
}

var project = solution.Projects.FirstOrDefault(p => string.Equals(p.Name, projectName, StringComparison.OrdinalIgnoreCase))
    ?? solution.Projects.FirstOrDefault(p => string.Equals(Path.GetFileNameWithoutExtension(p.FilePath), projectName, StringComparison.OrdinalIgnoreCase));

if (project is null)
{
    return Fail($"Project not found in solution: {projectName}");
}

Compilation compilation = (await project.GetCompilationAsync())
    ?? throw new InvalidOperationException("Failed to get compilation.");

string? nugetPackages = Environment.GetEnvironmentVariable("NUGET_PACKAGES");
if (string.IsNullOrWhiteSpace(nugetPackages))
{
    nugetPackages = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".nuget", "packages");
}

string analyzerAssemblyPath = Path.Combine(
    nugetPackages,
    "microsoft.codeanalysis.publicapianalyzers",
    "3.3.4",
    "analyzers",
    "dotnet",
    "cs",
    "Microsoft.CodeAnalysis.PublicApiAnalyzers.dll");

if (!File.Exists(analyzerAssemblyPath))
{
    return Fail($"Analyzer assembly not found: {analyzerAssemblyPath}\nUpdate the generator tool to match the analyzer package version in the library.");
}

Assembly analyzerAssembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(analyzerAssemblyPath);

Type? generatorType = analyzerAssembly.GetType("Microsoft.CodeAnalysis.PublicApiAnalyzers.PublicApiGenerator", throwOnError: false)
    ?? analyzerAssembly.GetTypes().FirstOrDefault(t => t.Name is "PublicApiGenerator" or "PublicApiGenerator" && t.FullName?.Contains("PublicApi", StringComparison.OrdinalIgnoreCase) == true);

if (generatorType is null)
{
    return Fail("Could not find PublicApiGenerator type in analyzer assembly.");
}

MethodInfo? generatorMethod = generatorType
    .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
    .Where(m => m.ReturnType == typeof(string))
    .OrderByDescending(m => m.IsPublic)
    .FirstOrDefault(m =>
    {
        if (!m.Name.Contains("Generate", StringComparison.OrdinalIgnoreCase) &&
            !m.Name.Contains("Get", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var ps = m.GetParameters();
        if (ps.Length < 1 || ps.Length > 3)
        {
            return false;
        }

        bool hasSymbol = ps.Any(p => typeof(ISymbol).IsAssignableFrom(p.ParameterType));
        bool hasCompilation = ps.Any(p => typeof(Compilation).IsAssignableFrom(p.ParameterType));
        return hasSymbol && hasCompilation;
    });

if (generatorMethod is null)
{
    return Fail("Could not find a usable generator method on PublicApiGenerator.");
}

ISymbol symbol = compilation.Assembly;

object?[] invokeArgs = generatorMethod.GetParameters().Select(p =>
{
    if (typeof(ISymbol).IsAssignableFrom(p.ParameterType))
    {
        return symbol;
    }

    if (typeof(Compilation).IsAssignableFrom(p.ParameterType))
    {
        return compilation;
    }

    if (p.ParameterType == typeof(CancellationToken))
    {
        return CancellationToken.None;
    }

    return p.HasDefaultValue ? p.DefaultValue : null;
}).ToArray();

string apiText = (string)(generatorMethod.Invoke(null, invokeArgs)
    ?? throw new InvalidOperationException("Public API generator returned null."));

string projectDir = Path.GetDirectoryName(project.FilePath)
    ?? throw new InvalidOperationException("Project directory not found.");

string shippedPath = Path.Combine(projectDir, "PublicAPI.Shipped.txt");
string unshippedPath = Path.Combine(projectDir, "PublicAPI.Unshipped.txt");

static string NormalizeNewlines(string s) => s.Replace("\r\n", "\n").Replace("\r", "\n");

apiText = NormalizeNewlines(apiText).Trim();

var lines = apiText.Length == 0
    ? Array.Empty<string>()
    : apiText.Split('\n', StringSplitOptions.RemoveEmptyEntries)
        .Select(l => l.TrimEnd())
        .Where(l => l.Length > 0)
        .Distinct(StringComparer.Ordinal)
        .OrderBy(l => l, StringComparer.Ordinal)
        .ToArray();

string shippedContents = "#nullable enable\n" + string.Join("\n", lines) + (lines.Length == 0 ? "" : "\n");
string unshippedContents = "#nullable enable\n";

Directory.CreateDirectory(projectDir);
File.WriteAllText(shippedPath, shippedContents);
File.WriteAllText(unshippedPath, unshippedContents);

Console.WriteLine($"Wrote {lines.Length} public API lines to:\n- {shippedPath}\n- {unshippedPath}");
return 0;
