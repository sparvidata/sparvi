import sys
import re
import os
from pathlib import Path


def validate_semver_increase(old_version, new_version):
    """Verify that new_version is higher than old_version following semver rules."""
    if old_version == new_version:
        return False, "Version not changed"

    old_parts = list(map(int, old_version.split('.')))
    new_parts = list(map(int, new_version.split('.')))

    # Check major version
    if new_parts[0] > old_parts[0]:
        return True, "Major version increased"
    elif new_parts[0] < old_parts[0]:
        return False, "Major version decreased"

    # Check minor version
    if new_parts[1] > old_parts[1]:
        return True, "Minor version increased"
    elif new_parts[1] < old_parts[1]:
        return False, "Minor version decreased"

    # Check patch version
    if new_parts[2] > old_parts[2]:
        return True, "Patch version increased"
    elif new_parts[2] < old_parts[2]:
        return False, "Patch version decreased"

    return False, "Version not changed"


def validate_changelog_format(changelog_path):
    """Ensure the changelog follows the expected format."""
    with open(changelog_path, 'r') as f:
        content = f.read()

    # Check if the changelog has a new entry
    version_pattern = r"## \[\d+\.\d+\.\d+\] - \d{4}-\d{2}-\d{2}"
    if not re.search(version_pattern, content):
        return False, "No properly formatted version entry found"

    # Check if each version has subsections
    sections = ["### Added", "### Changed", "### Fixed"]
    for section in sections:
        if section not in content:
            return False, f"Missing {section} section"

    return True, "Changelog format is valid"


def main():
    repo_root = Path(__file__).parent.parent.parent

    # Check Python version file
    version_file = repo_root / "backend" / "data_quality_engine" / "version.py"
    if version_file.exists():
        # Extract current version
        with open(version_file, 'r') as f:
            content = f.read()
            version_match = re.search(r'__version__\s*=\s*[\'"]([^\'"]+)[\'"]', content)
            if not version_match:
                print("❌ Could not find version string in version.py")
                sys.exit(1)

    # Check changelog
    changelog_path = repo_root / "CHANGELOG.md"
    if changelog_path.exists():
        valid, message = validate_changelog_format(changelog_path)
        if not valid:
            print(f"❌ {message}")
            sys.exit(1)
        print(f"✅ {message}")
    else:
        print("❌ CHANGELOG.md not found")
        sys.exit(1)

    print("✅ All version and changelog checks passed")
    sys.exit(0)


if __name__ == "__main__":
    main()