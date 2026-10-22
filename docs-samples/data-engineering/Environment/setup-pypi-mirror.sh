#!/usr/bin/env bash
# =============================================================================
# setup-pypi-mirror.sh
#
# End-to-end setup of a custom PyPI mirror for Microsoft Fabric Spark in a
# DEP-protected workspace. Wraps every step from the public Learn doc:
#
#   1. Install Bandersnatch in a venv
#   2. Resolve the transitive dependency closure for your requested packages
#   3. Build a Bandersnatch config with an allowlist pinning the top-N
#      stable versions per project
#   4. Mirror to a local web/ tree
#   5. Smoke-test the local mirror with a throwaway venv + http.server
#   6. Rewrite ../../ URLs to absolute blob URLs
#   7. Configure the storage account (Static Website, indexDocument,
#      anonymous blob access on the packages container)
#   8. Upload the mirror to the storage account (two containers)
#      including pre-upload "leftover misplaced folders" detection and
#      post-upload mapping verification
#   9. Public smoke test from the storage URL
#
# STORAGE LAYOUT:
#   $web                : simple/, pypi/, json/, local-stats/  (text indexes)
#   <pkg-container>     : packages/                            (wheel binaries)
#
# REQUIRED:  Linux or WSL (bandersnatch is POSIX-only)
#            Python 3.10+ with venv
#            az CLI with a logged-in identity that has
#              "Storage Blob Data Contributor" on the storage account
#            curl
#
# USAGE:
#   ./setup-pypi-mirror.sh \
#       --acct      <storage-account-name> \
#       --rg        <resource-group-name> \
#       --sub       <subscription-id> \
#       --packages  "pydeequ six numpy pandas emoji fuzzywuzzy pytest" \
#       --runtime   1.3
#
# Optional:
#   --pkg-container  <name>          name of the packages container (default: source-packages)
#   --runtime        1.3|2.0         Fabric runtime; sets python+platform defaults (default: 1.3)
#   --python         3.11|3.13  override target Python (default: derived from --runtime)
#   --platform       <tag>           override wheel platform (default: manylinux_2_17_x86_64)
#   --keep           <n>             versions per project (default 2)
#   --work           ~/lm-pypi-mirror
#   --skip-smoke                     skip step 5 (local smoke test)
#   --skip-upload                    stop after step 6 (useful for review)
#   --rebuild                        wipe the mirror dir and start clean
#
# PER-RUNTIME DEFAULTS (matches the Per-runtime values table in the doc):
#   Runtime 1.3 (Spark 3.5)  -> Python 3.11, manylinux_2_17_x86_64
#   Runtime 2.0 (Spark 4.1)  -> Python 3.13, manylinux_2_17_x86_64
#
# After completion, point Fabric at:
#   https://<acct>.z<n>.web.core.windows.net/simple
# (zN comes from `az storage account show ... --query primaryEndpoints.web`)
# =============================================================================

set -euo pipefail

# ---------- defaults -----------------------------------------------------------
ACCT=""
RG=""
SUB=""
PACKAGES=""
PKG_CONTAINER="source-packages"
RUNTIME="1.3"
TARGET_PYTHON=""              # derived from RUNTIME if not set explicitly
TARGET_PLATFORM="manylinux_2_17_x86_64"
KEEP=2
WORK="$HOME/lm-pypi-mirror"
SKIP_SMOKE=0
SKIP_UPLOAD=0
REBUILD=0

# ---------- helpers ------------------------------------------------------------
log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m    [OK]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m    [WARN]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m    [ERR]\033[0m %s\n' "$*" >&2; exit 1; }

usage() {
  sed -n '2,55p' "$0"
  exit 1
}

# ---------- arg parsing --------------------------------------------------------
while (($#)); do
  case "$1" in
    --acct)            ACCT=$2;            shift 2 ;;
    --rg)              RG=$2;              shift 2 ;;
    --sub)             SUB=$2;             shift 2 ;;
    --packages)        PACKAGES=$2;        shift 2 ;;
    --pkg-container)   PKG_CONTAINER=$2;   shift 2 ;;
    --runtime)         RUNTIME=$2;         shift 2 ;;
    --python)          TARGET_PYTHON=$2;   shift 2 ;;
    --platform)        TARGET_PLATFORM=$2; shift 2 ;;
    --keep)            KEEP=$2;            shift 2 ;;
    --work)            WORK=$2;            shift 2 ;;
    --skip-smoke)      SKIP_SMOKE=1;       shift   ;;
    --skip-upload)     SKIP_UPLOAD=1;      shift   ;;
    --rebuild)         REBUILD=1;          shift   ;;
    -h|--help)         usage              ;;
    *) die "Unknown arg: $1 (run with --help)" ;;
  esac
done

[[ -n "$ACCT" ]]     || die "--acct is required"
[[ -n "$RG" ]]       || die "--rg is required"
[[ -n "$SUB" ]]      || die "--sub is required"
[[ -n "$PACKAGES" ]] || die "--packages is required (space-separated, quoted)"
[[ -n "$PKG_CONTAINER" ]] || die "--pkg-container cannot be empty"

# Derive python version from --runtime if not explicitly overridden.
if [[ -z "$TARGET_PYTHON" ]]; then
  case "$RUNTIME" in
    1.2)  TARGET_PYTHON="3.10" ;;
    1.3)  TARGET_PYTHON="3.11" ;;
    2.0)  TARGET_PYTHON="3.13" ;;
    *)    die "--runtime must be one of: 1.2, 1.3, 2.0 (got '$RUNTIME'). Or pass --python explicitly." ;;
  esac
fi

echo "Configuration:"
echo "  storage account : $ACCT (rg: $RG)"
echo "  subscription    : $SUB"
echo "  packages cont.  : $PKG_CONTAINER  (the other container is the auto-created \$web)"
echo "  fabric runtime  : $RUNTIME  ->  Python $TARGET_PYTHON, $TARGET_PLATFORM"
echo "  root packages   : $PACKAGES"
echo "  versions/pkg    : $KEEP"
echo "  work dir        : $WORK"
echo

# ---------- preflight ----------------------------------------------------------
log "Preflight checks"

command -v python3 >/dev/null || die "python3 not found on PATH"
command -v az >/dev/null      || die "az CLI not found on PATH (https://aka.ms/InstallAzureCLIDeb)"
command -v curl >/dev/null    || die "curl not found on PATH"

if [[ "$(uname -s)" != "Linux" ]]; then
  die "Bandersnatch is Linux-only. Run this inside WSL Ubuntu or an Azure Linux VM."
fi

if ! az account show >/dev/null 2>&1; then
  die "az is not logged in. Run: az login"
fi

ok "python3 $(python3 --version | awk '{print $2}')"
ok "az logged in as $(az account show --query user.name -o tsv)"

log "Setting subscription"
az account set --subscription "$SUB"
CUR_SUB=$(az account show --query name -o tsv)
ok "active subscription: $CUR_SUB"

log "Verifying storage account exists and is reachable"
STATIC_URL=$(az storage account show --name "$ACCT" -g "$RG" \
              --query primaryEndpoints.web -o tsv 2>/dev/null) \
    || die "Could not find storage account '$ACCT' in resource group '$RG'. Check spelling and your subscription."
[[ -n "$STATIC_URL" ]] || die "Static website is not enabled on '$ACCT'. Enable it in the portal: Storage account -> Capabilities -> Static website -> Enabled."
ok "static-website URL: $STATIC_URL"

# Container existence check / creation for the packages container.
if ! az storage container show \
      --account-name "$ACCT" --name "$PKG_CONTAINER" \
      --auth-mode login >/dev/null 2>&1; then
  log "Creating packages container '$PKG_CONTAINER' with anonymous blob read"
  az storage account update --name "$ACCT" -g "$RG" \
      --allow-blob-public-access true --only-show-errors >/dev/null
  az storage container create \
      --account-name "$ACCT" --name "$PKG_CONTAINER" \
      --public-access blob --auth-mode login >/dev/null
  ok "created container '$PKG_CONTAINER'"
else
  ok "container '$PKG_CONTAINER' already exists"
fi

# ---------- step 1: install bandersnatch --------------------------------------
log "Step 1/9: installing bandersnatch in $WORK/venv"

if [[ $REBUILD -eq 1 ]]; then
  warn "--rebuild given; removing $WORK"
  rm -rf "$WORK"
fi
mkdir -p "$WORK"
cd "$WORK"

if [[ ! -d venv ]]; then
  python3 -m venv venv
fi
# shellcheck disable=SC1091
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet bandersnatch

BS_VERSION=$(bandersnatch --version 2>&1 | awk '{print $NF}' | head -1)
ok "bandersnatch $BS_VERSION installed"

# ---------- step 2: resolve closure -------------------------------------------
log "Step 2/9: resolving dependency closure for: $PACKAGES"
log "         (target: Python $TARGET_PYTHON, $TARGET_PLATFORM)"

mkdir -p build
pip install --quiet --dry-run --ignore-installed \
    --report build/report.json --only-binary=:all: \
    --python-version "$TARGET_PYTHON" \
    --platform       "$TARGET_PLATFORM" \
    --target         /tmp/lm-pypi-discover \
    $PACKAGES >/dev/null

python3 - <<'PY' > build/closure.txt
import json
d = json.load(open("build/report.json"))
seen = set()
for it in d["install"]:
    seen.add(it["metadata"]["name"].lower())
for n in sorted(seen):
    print(n)
PY

CLOSURE_COUNT=$(wc -l < build/closure.txt)
ok "$CLOSURE_COUNT projects in closure:"
sed 's/^/      - /' build/closure.txt

# ---------- step 3: build allowlist + bandersnatch.conf -----------------------
log "Step 3/9: picking top-$KEEP stable versions per project and writing bandersnatch.conf"

KEEP=$KEEP python3 - <<'PY' > build/pinned_versions.txt
import os, json, urllib.request
from packaging.version import Version, InvalidVersion

keep = int(os.environ["KEEP"])
projects = [line.strip() for line in open("build/closure.txt") if line.strip()]

for pkg in projects:
    try:
        data = json.loads(urllib.request.urlopen(f"https://pypi.org/pypi/{pkg}/json").read())
    except Exception as e:
        print(f"# WARNING: skipped {pkg} (PyPI fetch failed: {e})", flush=True)
        continue
    versions = []
    for v, files in data["releases"].items():
        if not files or any(f.get("yanked") for f in files):
            continue
        try:
            vobj = Version(v)
        except InvalidVersion:
            continue   # skip non-PEP-440 legacy strings (e.g. pytz "2004d")
        if vobj.is_prerelease or vobj.is_devrelease:
            continue
        versions.append((vobj, v))
    versions.sort(reverse=True)
    for _, v in versions[:keep]:
        print(f"{pkg}=={v}")
PY

PIN_COUNT=$(grep -cv '^#' build/pinned_versions.txt)
ok "$PIN_COUNT pinned (project, version) pairs written to build/pinned_versions.txt"

# Indent each line by 4 spaces for the .conf [allowlist] block.
ALLOWLIST_BLOCK=$(sed 's/^/    /' build/pinned_versions.txt)

cat > bandersnatch.conf <<EOF
; Auto-generated by setup-pypi-mirror.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)
; Target runtime: Python $TARGET_PYTHON, $TARGET_PLATFORM
; Closure derived from: $PACKAGES

[mirror]
directory = $WORK/mirror
master = https://pypi.org

json = true
release-files = true
hash-index = false
simple-format = ALL

storage-backend = filesystem
api-method = simple
workers = 3
verifiers = 3
timeout = 60
global-timeout = 18000
cleanup = false
stop-on-error = false
compare-method = hash

[plugins]
enabled =
    allowlist_project
    allowlist_release
    exclude_platform

[allowlist]
; ${PIN_COUNT} pinned (project, version) lines. Edit and re-run to refresh.
packages =
${ALLOWLIST_BLOCK}

[blocklist]
platforms =
    windows
    macos
    freebsd
EOF
ok "wrote bandersnatch.conf"

# ---------- step 4: run mirror -------------------------------------------------
log "Step 4/9: running bandersnatch (will fetch new/changed files only on re-runs)"
mkdir -p mirror
bandersnatch --config bandersnatch.conf mirror 2>&1 \
    | grep -E 'Initialized|allowlisted|Storing|had changes|ERROR|Traceback' \
    | sed 's/^/    /' || true

SIZE=$(du -sh mirror/web 2>/dev/null | awk '{print $1}')
WHEELS=$(find mirror/web/packages -name '*.whl' 2>/dev/null | wc -l)
SDISTS=$(find mirror/web/packages -name '*.tar.gz' 2>/dev/null | wc -l)
ok "mirror size: $SIZE  ($WHEELS wheels, $SDISTS sdists)"

# Sanity: every project in closure must appear in simple/.
MISSING=()
for pkg in $(cat build/closure.txt); do
  if [[ ! -d "mirror/web/simple/$pkg" ]]; then
    MISSING+=("$pkg")
  fi
done
if (( ${#MISSING[@]} > 0 )); then
  warn "Projects missing from simple/ (check for InvalidVersion errors above):"
  for p in "${MISSING[@]}"; do warn "  - $p"; done
fi

# ---------- step 5: local smoke test ------------------------------------------
if [[ $SKIP_SMOKE -eq 0 ]]; then
  log "Step 5/9: local smoke test (python3 -m http.server + pip install in /tmp venv)"
  (cd mirror/web && python3 -m http.server 8765 >/tmp/lm-pypi-http.log 2>&1) &
  SRV=$!
  trap "kill $SRV 2>/dev/null || true" EXIT
  sleep 2
  curl -sf http://localhost:8765/simple/ >/dev/null || die "local http.server did not come up"

  rm -rf /tmp/lm-pypi-smoke
  python3 -m venv /tmp/lm-pypi-smoke
  # shellcheck disable=SC1091
  source /tmp/lm-pypi-smoke/bin/activate
  pip install --quiet --upgrade pip
  if pip install --quiet --index-url http://localhost:8765/simple/ $PACKAGES; then
    ok "pip install $PACKAGES from local mirror: OK"
  else
    die "pip install from local mirror failed"
  fi
  deactivate
  kill $SRV 2>/dev/null || true
  trap - EXIT

  # Re-activate the build venv for downstream steps.
  # shellcheck disable=SC1091
  source venv/bin/activate
else
  warn "Step 5/9: SKIPPED (--skip-smoke)"
fi

# ---------- step 6: URL rewrite -----------------------------------------------
log "Step 6/9: rewriting ../../ -> https://$ACCT.blob.core.windows.net/$PKG_CONTAINER/"

if grep -rq "$ACCT.blob.core.windows.net" mirror/web/simple 2>/dev/null; then
  warn "  detected previous rewrite to this account; nothing to do."
else
  python3 - <<PY
import os
old = "../../"
new = "https://$ACCT.blob.core.windows.net/$PKG_CONTAINER/"
INDEX = {"index.html", "index.v1_html", "index.v1_json"}
n_files = n_subs = 0
for root, _, files in os.walk("mirror/web"):
    for name in files:
        if name not in INDEX:
            continue
        fp = os.path.join(root, name)
        with open(fp, "r", encoding="utf-8") as f:
            data = f.read()
        new_data = data.replace(old, new)
        if new_data != data:
            with open(fp, "w", encoding="utf-8") as f:
                f.write(new_data)
            n_files += 1
            n_subs += new_data.count(new) - data.count(new)
print(f"    rewrote {n_subs} URLs across {n_files} files")
PY
  ok "URL rewrite done"
fi

if [[ $SKIP_UPLOAD -eq 1 ]]; then
  warn "Stopping before upload (--skip-upload). Inspect $WORK/mirror/web/ before re-running."
  exit 0
fi

# ---------- step 7: storage-account settings ----------------------------------
log "Step 7/9: ensuring static-website indexDocument = 'index.html'"
CURRENT_IDX=$(az storage blob service-properties show --account-name "$ACCT" \
                --auth-mode login --query "staticWebsite.indexDocument" -o tsv 2>/dev/null || echo "")
if [[ "$CURRENT_IDX" != "index.html" ]]; then
  az storage blob service-properties update --account-name "$ACCT" \
      --auth-mode login --static-website --index-document index.html \
      --only-show-errors >/dev/null
  ok "set indexDocument = index.html (was: '${CURRENT_IDX:-null}')"
else
  ok "indexDocument already set to index.html"
fi

# Make sure account-level public access is enabled (idempotent).
az storage account update --name "$ACCT" -g "$RG" \
    --allow-blob-public-access true --only-show-errors >/dev/null
ok "account-level allowBlobPublicAccess = true"

# ---------- step 8: upload -----------------------------------------------------
log "Step 8/9: uploading mirror to '$ACCT' (packages -> $PKG_CONTAINER; indexes -> \$web)"

SRC="$WORK/mirror/web"

# Pre-upload check: detect leftover misplaced folders from a previous broken
# setup. upload-batch only writes — it never deletes — so stale misplaced
# folders survive an otherwise-correct re-upload and break pip lookups.
# This mirrors the "Folders ended up in the wrong container" troubleshooting
# entry in the doc.
log "  pre-upload mapping check"

WEB_HAS_PACKAGES=$(az storage blob list \
    --account-name "$ACCT" --container-name '$web' \
    --prefix packages/ --auth-mode login \
    --query "length(@)" -o tsv 2>/dev/null || echo "0")
if [[ "$WEB_HAS_PACKAGES" != "0" ]]; then
  warn "  \$web already contains $WEB_HAS_PACKAGES blob(s) under packages/ from a previous run."
  warn "  packages/ MUST live in '$PKG_CONTAINER' instead. Delete it from \$web first:"
  warn "      az storage blob delete-batch --account-name $ACCT \\"
  warn "          --source '\$web' --pattern 'packages/*' --auth-mode login"
  warn "  Then re-run this script."
  die "pre-upload check failed: packages/ found in \$web"
fi

PKG_HAS_STRAY=$(az storage blob list \
    --account-name "$ACCT" --container-name "$PKG_CONTAINER" \
    --auth-mode login --query "[?!starts_with(name, 'packages/')].name" -o tsv 2>/dev/null \
    | head -5)
if [[ -n "$PKG_HAS_STRAY" ]]; then
  warn "  '$PKG_CONTAINER' contains blobs that are NOT under packages/:"
  while IFS= read -r line; do warn "    $line"; done <<< "$PKG_HAS_STRAY"
  warn "  Only packages/<hash>/<file> should live in '$PKG_CONTAINER'."
  warn "  If those stray blobs are leftover simple/, pypi/, json/, etc. from a previous run,"
  warn "  delete them with:"
  warn "      az storage blob delete-batch --account-name $ACCT \\"
  warn "          --source $PKG_CONTAINER --pattern '<bad-prefix>/*' --auth-mode login"
  warn "  Then re-run this script."
  die "pre-upload check failed: non-packages blobs in '$PKG_CONTAINER'"
fi
ok "  pre-upload mapping check passed"

log "  uploading packages -> $PKG_CONTAINER (this is the bulk of the bytes)"
az storage blob upload-batch \
    --account-name "$ACCT" \
    --destination "$PKG_CONTAINER" \
    --destination-path packages \
    --source "$SRC/packages" \
    --auth-mode login --overwrite --max-connections 16 \
    --output none --only-show-errors

for sub in simple pypi local-stats json; do
  [[ -d "$SRC/$sub" ]] || { warn "  skipping $sub (not present)"; continue; }
  echo "    uploading $sub -> \$web ..."
  az storage blob upload-batch \
      --account-name "$ACCT" \
      --destination '$web' \
      --destination-path "$sub" \
      --source "$SRC/$sub" \
      --auth-mode login --overwrite --max-connections 16 \
      --output none --only-show-errors
done
ok "upload complete"

# Post-upload verification: top-level entries in each container should match
# the expected mapping (doc Troubleshooting -> "Folders ended up in the wrong
# container").
log "  post-upload mapping check"
get_top_prefixes() {
  local container="$1"
  az storage blob list --account-name "$ACCT" \
      --container-name "$container" --auth-mode login \
      --query "[].name" -o tsv 2>/dev/null \
      | awk -F/ 'NF>1 {print $1}' | sort -u
}

WEB_TOP=$(get_top_prefixes '$web')
PKG_TOP=$(get_top_prefixes "$PKG_CONTAINER")

echo "    \$web top-level prefixes:           $(echo "$WEB_TOP" | tr '\n' ' ')"
echo "    $PKG_CONTAINER top-level prefixes:  $(echo "$PKG_TOP" | tr '\n' ' ')"

EXPECTED_WEB="json local-stats pypi simple"
EXPECTED_PKG="packages"
ACTUAL_WEB=$(echo "$WEB_TOP" | sort -u | tr '\n' ' ' | sed 's/ $//')
ACTUAL_PKG=$(echo "$PKG_TOP" | sort -u | tr '\n' ' ' | sed 's/ $//')

VERIFY_FAIL=0
if [[ "$ACTUAL_WEB" != "$EXPECTED_WEB" ]]; then
  warn "  \$web mapping mismatch:"
  warn "    expected: $EXPECTED_WEB"
  warn "    actual  : $ACTUAL_WEB"
  VERIFY_FAIL=1
fi
if [[ "$ACTUAL_PKG" != "$EXPECTED_PKG" ]]; then
  warn "  $PKG_CONTAINER mapping mismatch:"
  warn "    expected: $EXPECTED_PKG"
  warn "    actual  : $ACTUAL_PKG"
  VERIFY_FAIL=1
fi

if (( VERIFY_FAIL == 0 )); then
  WEB_COUNT=$(az storage blob list --account-name "$ACCT" --container-name '$web' \
                --auth-mode login --query "length(@)" -o tsv)
  PKG_COUNT=$(az storage blob list --account-name "$ACCT" --container-name "$PKG_CONTAINER" \
                --auth-mode login --query "length(@)" -o tsv)
  ok "  mapping verified: \$web has $WEB_COUNT blobs (indexes), $PKG_CONTAINER has $PKG_COUNT blobs (wheels)"
else
  die "post-upload mapping check failed. See doc Troubleshooting -> 'Folders ended up in the wrong container' for repair steps."
fi

# ---------- step 9: public smoke test -----------------------------------------
log "Step 9/9: public smoke test via $STATIC_URL"

STATIC_BASE=${STATIC_URL%/}
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$STATIC_BASE/simple/")
if [[ "$HTTP_CODE" != "200" ]]; then
  die "GET $STATIC_BASE/simple/ returned HTTP $HTTP_CODE (expected 200). \
Check: (a) static-website indexDocument is index.html (step 7), \
       (b) upload of simple/ completed (step 8)."
fi
ok "GET /simple/ -> 200"

# Pick the first package from the user's request to smoke-test.
FIRST_PKG=$(echo "$PACKAGES" | awk '{print $1}')
HTTP_CODE_PKG=$(curl -s -o /dev/null -w '%{http_code}' "$STATIC_BASE/simple/$FIRST_PKG/")
ok "GET /simple/$FIRST_PKG/ -> $HTTP_CODE_PKG"

# Final smoke install from the public URL into a throwaway venv.
rm -rf /tmp/lm-pypi-public
python3 -m venv /tmp/lm-pypi-public
# shellcheck disable=SC1091
source /tmp/lm-pypi-public/bin/activate
pip install --quiet --upgrade pip
if pip install --quiet --index-url "$STATIC_BASE/simple/" $PACKAGES; then
  ok "pip install $PACKAGES from $STATIC_BASE/simple/ : SUCCESS"
  pip list 2>/dev/null | head -40 | sed 's/^/      /'
else
  die "pip install from the public URL failed."
fi
deactivate

# ---------- next steps --------------------------------------------------------
cat <<EOF

=========================================================================
DONE. The mirror is live at:

    --index-url $STATIC_BASE/simple

NEXT STEPS for Fabric (DEP-protected workspace):

  1. Workspace -> Manage -> Managed private endpoints -> + New
       (a) sub-resource: blob   ->  $ACCT
       (b) sub-resource: web    ->  $ACCT
     Approve both in Azure portal:
       Storage account -> Networking -> Private endpoint connections.

  2. Workspace -> New -> Environment.
     Add a 'Public libraries' YAML:

         dependencies:
           - pip:
             - $(echo "$PACKAGES" | tr ' ' '\n' | head -1)
             - --index-url $STATIC_BASE/simple

     Publish the environment, then attach it to your notebook
     (Notebook ribbon -> Environment -> pick the new one).

  3. In a notebook cell, verify the right copy is loaded:

         import $(echo "$PACKAGES" | awk '{print $1}'), sys
         print($(echo "$PACKAGES" | awk '{print $1}').__file__)
         print($(echo "$PACKAGES" | awk '{print $1}').__version__)

     The __file__ path should point under your environment artifact,
     NOT /home/trusted-service-user/cluster-env/...

=========================================================================
EOF
