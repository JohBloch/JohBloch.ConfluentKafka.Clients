using System.Collections.Immutable;
using System.Reflection;
using System.Runtime.Loader;
using System.Text;
using Microsoft.Build.Locator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.MSBuild;
using Microsoft.CodeAnalysis.Text;

static int Fail(string message)
{
    Console.Error.WriteLine(message);
    return 1;
}

string repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
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

// Safety: ensure we only write outputs inside this repository.
// This tool is intended for this repo; allowing arbitrary solution paths can lead to writing files outside the repo.
solutionPath = Path.GetFullPath(solutionPath);
var repoRootWithSep = repoRoot.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
if (!solutionPath.StartsWith(repoRootWithSep, StringComparison.OrdinalIgnoreCase))
{
    return Fail($"Refusing to open solution outside repoRoot. repoRoot='{repoRoot}', solution='{solutionPath}'");
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

string projectDir = Path.GetDirectoryName(project.FilePath)
    ?? throw new InvalidOperationException("Project directory not found.");

projectDir = Path.GetFullPath(projectDir);
if (!projectDir.StartsWith(repoRootWithSep, StringComparison.OrdinalIgnoreCase))
{
    return Fail($"Refusing to write PublicAPI files outside repoRoot. repoRoot='{repoRoot}', projectDir='{projectDir}'");
}

string shippedPath = Path.Combine(projectDir, "PublicAPI.Shipped.txt");
string unshippedPath = Path.Combine(projectDir, "PublicAPI.Unshipped.txt");

string analyzerAssemblyPath = Path.Combine(
    nugetPackages,
    "microsoft.codeanalysis.publicapianalyzers",
    "3.3.4",
    "analyzers",
    "dotnet",
    "Microsoft.CodeAnalysis.PublicApiAnalyzers.dll");

if (!File.Exists(analyzerAssemblyPath))
{
    return Fail($"Analyzer assembly not found: {analyzerAssemblyPath}\nUpdate the generator tool to match the analyzer package version in the library.");
}

Assembly analyzerAssembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(analyzerAssemblyPath);

static IEnumerable<ISymbol> EnumerateSymbols(IAssemblySymbol assembly)
{
    foreach (var symbol in EnumerateNamespace(assembly.GlobalNamespace))
    {
        yield return symbol;
    }

    static IEnumerable<ISymbol> EnumerateNamespace(INamespaceSymbol ns)
    {
        foreach (var member in ns.GetMembers())
        {
            if (member is INamespaceSymbol nestedNs)
            {
                foreach (var nested in EnumerateNamespace(nestedNs))
                {
                    yield return nested;
                }

                continue;
            }

            if (member is INamedTypeSymbol type)
            {
                foreach (var nested in EnumerateType(type))
                {
                    yield return nested;
                }
            }
        }
    }

    static IEnumerable<ISymbol> EnumerateType(INamedTypeSymbol type)
    {
        yield return type;

        foreach (var member in type.GetMembers())
        {
            yield return member;

            if (member is INamedTypeSymbol nestedType)
            {
                foreach (var nested in EnumerateType(nestedType))
                {
                    yield return nested;
                }
            }
            else if (member is IPropertySymbol prop)
            {
                if (prop.GetMethod is not null)
                {
                    yield return prop.GetMethod;
                }

                if (prop.SetMethod is not null)
                {
                    yield return prop.SetMethod;
                }
            }
            else if (member is IEventSymbol ev)
            {
                if (ev.AddMethod is not null)
                {
                    yield return ev.AddMethod;
                }

                if (ev.RemoveMethod is not null)
                {
                    yield return ev.RemoveMethod;
                }
            }
        }
    }
}

static object GetImmutableArrayEmpty(Type elementType)
{
    var immutableArrayType = typeof(ImmutableArray<>).MakeGenericType(elementType);

    var emptyProp = immutableArrayType.GetProperty("Empty", BindingFlags.Public | BindingFlags.Static);
    if (emptyProp is not null)
    {
        return emptyProp.GetValue(null)!;
    }

    var emptyField = immutableArrayType.GetField("Empty", BindingFlags.Public | BindingFlags.Static);
    if (emptyField is not null)
    {
        return emptyField.GetValue(null)!;
    }

    // Fallback: default(ImmutableArray<T>) is empty.
    return Activator.CreateInstance(immutableArrayType)!;
}

static object CreateInstanceAllowNonPublic(Type type, params object?[] args)
{
    var ctors = type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
    foreach (var ctor in ctors)
    {
        var parameters = ctor.GetParameters();
        if (parameters.Length != args.Length)
        {
            continue;
        }

        bool match = true;
        for (int i = 0; i < parameters.Length; i++)
        {
            var pType = parameters[i].ParameterType;
            var arg = args[i];
            if (arg is null)
            {
                if (pType.IsValueType && Nullable.GetUnderlyingType(pType) is null)
                {
                    match = false;
                    break;
                }

                continue;
            }

            if (!pType.IsInstanceOfType(arg))
            {
                match = false;
                break;
            }
        }

        if (match)
        {
            return ctor.Invoke(args);
        }
    }

    throw new MissingMethodException($"No matching constructor found for {type.FullName}({string.Join(", ", args.Select(a => a?.GetType().FullName ?? "null"))}).");
}

var additionalTexts = ImmutableArray<AdditionalText>.Empty;
var analyzerOptions = new AnalyzerOptions(additionalTexts);

Type apiLineType = analyzerAssembly.GetType("Microsoft.CodeAnalysis.PublicApiAnalyzers.DeclarePublicApiAnalyzer+ApiLine", throwOnError: true)
    ?? throw new InvalidOperationException("ApiLine type not found.");
Type removedApiLineType = analyzerAssembly.GetType("Microsoft.CodeAnalysis.PublicApiAnalyzers.DeclarePublicApiAnalyzer+RemovedApiLine", throwOnError: true)
    ?? throw new InvalidOperationException("RemovedApiLine type not found.");
Type apiDataType = analyzerAssembly.GetType("Microsoft.CodeAnalysis.PublicApiAnalyzers.DeclarePublicApiAnalyzer+ApiData", throwOnError: true)
    ?? throw new InvalidOperationException("ApiData type not found.");
Type apiNameType = analyzerAssembly.GetType("Microsoft.CodeAnalysis.PublicApiAnalyzers.DeclarePublicApiAnalyzer+ApiName", throwOnError: true)
    ?? throw new InvalidOperationException("ApiName type not found.");
Type implType = analyzerAssembly.GetType("Microsoft.CodeAnalysis.PublicApiAnalyzers.DeclarePublicApiAnalyzer+Impl", throwOnError: true)
    ?? throw new InvalidOperationException("Impl type not found.");

object emptyApiLines = GetImmutableArrayEmpty(apiLineType);
object emptyRemovedApiLines = GetImmutableArrayEmpty(removedApiLineType);

object emptyApiData = CreateInstanceAllowNonPublic(apiDataType, emptyApiLines, emptyRemovedApiLines, 0);

// Constructor: Impl(Compilation, ApiData shipped, ApiData unshipped, bool isPublic, AnalyzerOptions)
object impl = CreateInstanceAllowNonPublic(implType, compilation, emptyApiData, emptyApiData, true, analyzerOptions);

MethodInfo isTrackedApiMethod = implType.GetMethod("IsTrackedAPI", BindingFlags.NonPublic | BindingFlags.Instance)
    ?? throw new InvalidOperationException("IsTrackedAPI method not found.");
MethodInfo getApiNameMethod = implType.GetMethod("GetApiName", BindingFlags.NonPublic | BindingFlags.Instance)
    ?? throw new InvalidOperationException("GetApiName method not found.");
PropertyInfo nameWithNullabilityProp = apiNameType.GetProperty("NameWithNullability", BindingFlags.Public | BindingFlags.Instance)
    ?? throw new InvalidOperationException("ApiName.NameWithNullability not found.");

var lines = new HashSet<string>(StringComparer.Ordinal);

foreach (var symbol in EnumerateSymbols(compilation.Assembly))
{
    if (symbol is IMethodSymbol { MethodKind: MethodKind.Constructor, ContainingType.TypeKind: TypeKind.Enum })
    {
        continue;
    }

    // Public API analyzers only track the public/protected surface.
    // This also prevents private compiler-generated symbols (e.g. enum constructors) from being declared.
    if (symbol.DeclaredAccessibility is not (Accessibility.Public or Accessibility.Protected or Accessibility.ProtectedOrInternal or Accessibility.ProtectedAndInternal))
    {
        continue;
    }

    bool isTracked = (bool)isTrackedApiMethod.Invoke(impl, new object?[] { symbol, CancellationToken.None })!;
    if (!isTracked)
    {
        continue;
    }

    object apiName = getApiNameMethod.Invoke(impl, new object?[] { symbol })!;
    string? text = (string?)nameWithNullabilityProp.GetValue(apiName);
    if (!string.IsNullOrWhiteSpace(text))
    {
        lines.Add(text.Trim());
    }
}

var orderedLines = lines.OrderBy(l => l, StringComparer.Ordinal).ToArray();

string shippedContents = "#nullable enable\n" + string.Join("\n", orderedLines) + (orderedLines.Length == 0 ? "" : "\n");
string unshippedContents = "#nullable enable\n";

Directory.CreateDirectory(projectDir);
File.WriteAllText(shippedPath, shippedContents, Encoding.UTF8);
File.WriteAllText(unshippedPath, unshippedContents, Encoding.UTF8);

Console.WriteLine($"Wrote {orderedLines.Length} public API lines to:\n- {shippedPath}\n- {unshippedPath}");
return 0;
