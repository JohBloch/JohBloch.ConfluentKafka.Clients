# Release Process

This document describes the process for releasing a new version of JohBloch.ConfluentKafka.Clients.

## Pre-Release Checklist

- [ ] All tests pass locally (`dotnet test`)
- [ ] All tests pass in CI (GitHub Actions)
- [ ] Code coverage is acceptable (target: >80%)
- [ ] Documentation is up to date (README.md, XML comments)
- [ ] CHANGELOG.md is updated with new version and changes
- [ ] Version number is updated in `.csproj` file
- [ ] No open critical or blocking issues
- [ ] All planned features for the release are complete

## Version Numbering

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR** version: Incompatible API changes
- **MINOR** version: New functionality in a backwards compatible manner
- **PATCH** version: Backwards compatible bug fixes

Examples:
- Bug fix: `1.0.0` → `1.0.1`
- New feature: `1.0.1` → `1.1.0`
- Breaking change: `1.1.0` → `2.0.0`

## Release Steps

### 1. Update Version

Update the version in `src/JohBloch.ConfluentKafka.Clients/JohBloch.ConfluentKafka.Clients.csproj`:

```xml
<Version>1.1.0</Version>
<PackageReleaseNotes>Description of changes in this release.</PackageReleaseNotes>
```

### 2. Update CHANGELOG.md

Move changes from `[Unreleased]` section to a new version section:

```markdown
## [1.1.0] - 2026-01-15

### Added
- New feature X
- New feature Y

### Fixed
- Bug fix Z
```

### 3. Commit and Tag

```powershell
git add .
git commit -m "Release v1.1.0"
git tag -a v1.1.0 -m "Release version 1.1.0"
git push origin main
git push origin v1.1.0
```

### 4. Create GitHub Release

1. Go to [GitHub Releases](https://github.com/JohBloch/JohBloch.ConfluentKafka.Clients/releases)
2. Click "Draft a new release"
3. Select the tag you just created (`v1.1.0`)
4. Title: `v1.1.0`
5. Description: Copy from CHANGELOG.md
6. Check "Set as the latest release"
7. Click "Publish release"

This will automatically trigger the `publish.yml` GitHub Actions workflow, which will:
- Build the project
- Run all tests
- Create NuGet package
- Publish to NuGet.org

### 5. Verify Release

1. Check that the GitHub Actions workflow completed successfully
2. Verify package appears on [NuGet.org](https://www.nuget.org/packages/JohBloch.ConfluentKafka.Clients/)
3. Test installation in a sample project:
   ```powershell
   dotnet add package JohBloch.ConfluentKafka.Clients --version 1.1.0
   ```

### 6. Announce Release

- Update project documentation if needed
- Announce on relevant channels (if applicable)
- Close milestone in GitHub (if using milestones)

## Hotfix Releases

For critical bug fixes that need immediate release:

1. Create hotfix branch from the release tag:
   ```powershell
   git checkout -b hotfix/1.0.1 v1.0.0
   ```

2. Make the fix and update version to patch level (`1.0.1`)

3. Follow steps 2-6 above

4. Merge hotfix back to main:
   ```powershell
   git checkout main
   git merge hotfix/1.0.1
   git push origin main
   ```

## Pre-Release Versions

For alpha/beta releases, use pre-release version suffixes:

```xml
<Version>1.1.0-beta.1</Version>
```

When creating the GitHub release, check "Set as a pre-release".

## Troubleshooting

### NuGet Publish Failed

1. Check that `NUGET_API_KEY` secret is set correctly in GitHub repository settings
2. Verify the API key is still valid on NuGet.org
3. Check GitHub Actions logs for specific error messages

### Tests Failed in CI

1. Review GitHub Actions logs to identify failing tests
2. Fix issues locally and ensure tests pass
3. Commit fixes and re-tag if necessary

### Version Already Published

NuGet.org doesn't allow overwriting published versions. If you need to fix a release:
1. Delete the GitHub release and tag
2. Increment to next patch version
3. Follow release process again

## Rollback

If a release has critical issues:

1. **Do not delete from NuGet** - this breaks dependent projects
2. Publish a new patch version with fixes
3. Mark the problematic version as deprecated (if possible)
4. Update documentation with migration guide
