"""Build only the source/sample allowlist, never runtime state or credentials."""

import argparse
import hashlib
import json
import re
import zipfile
from pathlib import Path

from setup_utility_outages import ROOT, SAMPLES, verify_package


FILES = (
    "README.md", "LICENSE", ".gitignore", "requirements.txt",
    "setup_utility_outages.py", "CurrentOutageConditions.kql",
    "verify-handoff.kql", "verify-history.sql", "package_setup.py",
    "tests/test_setup.py",
) + tuple("data/" + name for name in SAMPLES)
ZIP_NAME = "utility-outage-monitoring-maps-first-setup.zip"


def make_manifest() -> dict:
    return {"format_version": 1, "package": "utility-outage-monitoring-maps-first-setup",
            "source_assets": "Seven unchanged files from the adjacent original tutorial sample directory",
            "original_sample_zip_sha256": "63b618a8b0aa2a133c0ac2e91ff69b431f11e708fd101bca27ff4795c03fea17",
            "sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                       for name in sorted(FILES)}}


def check_content(content: bytes, name: str) -> None:
    text = content.decode("utf-8-sig")
    # Heuristics supplement the explicit allowlist and manual review; not a DLP claim.
    patterns = [r"eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{15,}",
                r"-----BEGIN (?:RSA |EC )?PRIVATE KEY-----",
                r"SharedAccessKey=[A-Za-z0-9+/]{20,}",
                r"(?i)[a-z]:\\(?:users|github)\\"]
    if any(re.search(pattern, text) for pattern in patterns):
        raise ValueError(f"Possible credential or local path leak in {name}")


def build(destination: Path | None = None) -> Path:
    destination = destination or ROOT.parent / ZIP_NAME
    verify_package()
    manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))
    if manifest != make_manifest():
        raise ValueError("Manifest doesn't match the complete package allowlist.")
    contents = {name: (ROOT / name).read_bytes() for name in (*FILES, "manifest.json")}
    for name, content in contents.items():
        check_content(content, name)
    # Exclusive creation prevents replacing the original ZIP or an earlier build.
    with destination.open("xb") as stream:
        with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for name in sorted(contents):
                entry = zipfile.ZipInfo("maps-first-setup/" + name, date_time=(2026, 10, 21, 0, 0, 0))
                entry.compress_type = zipfile.ZIP_DEFLATED
                entry.external_attr = 0o100644 << 16
                archive.writestr(entry, contents[name])
    with zipfile.ZipFile(destination) as archive:
        expected = {"maps-first-setup/" + name for name in contents}
        if set(archive.namelist()) != expected or archive.testzip() is not None:
            raise ValueError("ZIP entry/CRC verification failed.")
        for name, content in contents.items():
            if archive.read("maps-first-setup/" + name) != content:
                raise ValueError("ZIP content verification failed.")
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print-manifest", action="store_true")
    args = parser.parse_args()
    if args.print_manifest:
        print(json.dumps(make_manifest(), indent=2))
    else:
        result = build()
        print(f"Created and verified {result.name}: {result.stat().st_size} bytes")