# Maps-first utility outage setup

Prepare the data used by **Build a location-aware utility outage monitoring map**,
then start that tutorial at **Create a utility outage monitoring map**.

**This package doesn't create any map items.** It also doesn't create a simulator,
an eventstream, a KQL queryset, a dashboard, an Activator item, a notebook item, an
Azure Maps account, or an app registration. It is a one-time, manually executed,
bounded dataset preparation, **not continuous real-time processing**. Refreshing a
map won't geocode new reports or run this setup.

## Contents and resulting resources

- `setup_utility_outages.py`: complete provisioning, enrichment, ingestion,
  verification, Spark persistence, and context-conversion implementation.
- `CurrentOutageConditions.kql`: the original tutorial's function, unchanged apart
  from indentation. It isn't a simplified or replacement grouping algorithm.
- `verify-handoff.kql` and `verify-history.sql`: read-only verification queries.
- `data/`: the seven original sample assets, copied without modifications.
- `tests/test_setup.py`: offline tests, with mocked service responses; no cloud calls.
- `manifest.json`: SHA-256 checksums for every other shipped file, including data.
- `package_setup.py`: reproducible archive builder and archive safety checks.
- `requirements.txt`, `.gitignore`, `LICENSE`, and this README.

The separate download is `utility-outage-monitoring-maps-first-setup.zip`. The
original `utility-outage-monitoring-sample-files.zip` is not replaced or nested
inside it. The unrelated `UtilityIncidents-test.csv` is deliberately not included.

| Resource | Prepared contents |
| --- | --- |
| `UtilityOutageEventhouse` | An eventhouse; minimum consumption settings aren't changed. |
| `UtilityOutageEventhouse` KQL database | An existing matching child is discovered, or a writable child is created. Actual parent ID, database name, and query/ingestion URIs come from Fabric APIs. |
| `RawIncidents` | Exactly 105 address-only incidents, using the tutorial's six-column lower-camel-case schema. |
| `EnrichedIncidents` | Exactly 105 incidents using the tutorial's 14-column schema, including `Matched`, `Review`, `NoMatch`, and `Error` records. |
| `KnownOutageAreas.geojson` KQL table | One FeatureCollection row with `type:string` and `features:dynamic`; it expands to `OUT-201`, `OUT-202`, and `OUT-203`. |
| `CurrentOutageConditions()` | The tutorial's level-14 S2 grouping, earliest-report-anchored 15-minute bins, known-polygon comparison, and priority scoring. |
| `UtilityOutagesLakehouse` | Created with `creationPayload.enableSchemas=true`; manually attach it as the notebook default before phase 2. |
| `dbo.incidenthistory` | Delta table with all 105 latest incident snapshots, including failed geocodes; not full change history. Two same-source upserts check duplicate prevention. |
| `Files/OperationalContext` | Four original GeoJSON files plus generated `RepairCrews.geojson` (7 points) and `NeighborhoodDistricts.geojson` (9 polygons). |
| `Files/UtilityOutageSetup/enriched-checkpoint.json` | Runtime-only recovery checkpoint: addresses, geocoding results, and source fingerprint. No tokens or credentials. Restrict access as for the incident data. |

`CurrentIncidents()` is intentionally **not** created: the map section creates it.
The lakehouse SQL analytics endpoint is provisioned by Fabric, not independently
created or written to by this code.

## Prerequisites and scope

1. A Fabric-enabled capacity and an interactive user with Contributor, Member, or
   Admin access to an **isolated tutorial workspace**. The identity must also have
   permissions to create KQL tables/functions, ingest data, and write the lakehouse.
   Workspace roles and OAuth permissions are distinct; an external application
   also needs delegated Fabric permissions such as `Item.ReadWrite.All`, with any
   required tenant consent. A role alone doesn't grant an app OAuth consent.
2. A Fabric **PySpark** notebook in that workspace with a supported Fabric Spark
   runtime, including its built-in Kusto connector and Delta library. Plain Python
   notebooks/local Python aren't sufficient for phase 2. Don't install `pyspark`
   or `delta-spark` over Fabric's runtime. Python source uses Python 3.10+ syntax.
3. An existing Azure Maps account and approved permission to send the supplied
   street addresses to it. Azure Maps transactions and Fabric capacity can incur
   charges. No Azure resource or role assignment is provisioned automatically.
4. **Microsoft Entra authentication for Azure Maps**. Assign **Azure Maps Data
   Reader** to the calling identity on the Maps account. Record the account's
   **Client ID** from its Authentication page; this goes in `azure_maps_client_id`
   and the `x-ms-client-id` header. It is NOT an app registration's application ID.
5. For the recommended interactive notebook flow, use an organization-approved
   public-client app registration configured for device-code authentication,
   with Azure Maps delegated `user_impersonation` permission and required consent.
   Your administrator may need to enable public-client flows. Supply its tenant ID
   and application/client ID to `DeviceCodeCredential`, **not** to the Maps account
   configuration. If Conditional Access blocks device code, use an approved
   `TokenCredential` instead; don't disable policy or embed keys to bypass it.

No secrets or subscription keys are required by this implementation. The notebook
flow uses documented NotebookUtils `pbi`/`kusto` tokens for Fabric/Kusto and a
**separate** Entra credential for Maps (`https://atlas.microsoft.com/.default`).
Fabric identity and Maps identity must each have their respective permissions;
using the same user is simplest. NotebookUtils tokens aren't assumed to have an
arbitrary Maps audience. Interactive notebook operation is the supported path;
service-principal pipelines aren't certified by this sample.

## Run in a Fabric notebook (recommended)

### 1. Upload the package and verify it offline

Create a PySpark notebook (for example, `PrepareUtilityOutageMapData`) in the target
workspace. Upload the **new ZIP** to **Explorer > Resources > built-in**. Keep the
ZIP filename unchanged. Its notebook path is
`builtin/utility-outage-monitoring-maps-first-setup.zip`.

Run this cell to extract into the notebook driver's temporary storage. This uses
the local package, not OneLake REST writes, and rejects path traversal. It refuses
to reuse an old extraction directory so code from different packages isn't mixed.
After a session restart, rerun it; if the directory already exists, restart the
session or choose a fresh temporary directory instead of overwriting it.

```python
from pathlib import Path
import sys
import tempfile
import zipfile

archive = Path("builtin/utility-outage-monitoring-maps-first-setup.zip")
extract_dir = Path(tempfile.mkdtemp(prefix="utility-outage-"))
with zipfile.ZipFile(archive) as package:
    for entry in package.infolist():
        target = (extract_dir / entry.filename).resolve()
        if not target.is_relative_to(extract_dir.resolve()):
            raise ValueError("Unsafe archive path")
    package.extractall(extract_dir)

package_dir = extract_dir / "maps-first-setup"
sys.path.insert(0, str(package_dir))
import setup_utility_outages as setup
setup.verify_package()
print("Package checksums verified; no cloud calls made.")
```

Install the two external dependencies in a separate cell:

```python
%pip install "httpx>=0.28.1,<1" "azure-identity>=1.25.1,<2"
```

If prompted to restart, restart and rerun the extraction/import cell. Fabric's
Spark, Kusto connector, `notebookutils`, and Delta libraries come from the runtime.
Don't publish notebook output containing a device-login code, address details, or
runtime credentials. Authenticate only through your organization's trusted login
page; never send a login code or secret to someone else.

### 2. Provision or discover the eventhouse, KQL database, and lakehouse

Replace the workspace and Maps account Client ID placeholders. Both are nonsecret
resource identifiers. The workspace ID is visible in the workspace URL. You may
use the notebook's current workspace ID as below; double-check it before running.

```python
import notebookutils

cfg = setup.Config(
    workspace_id=notebookutils.runtime.context["currentWorkspaceId"],
    azure_maps_client_id="<AZURE_MAPS_ACCOUNT_CLIENT_ID>",
    # Leave False for a clean workspace. See Existing resources and reruns.
    reuse_existing_items=False,
)
print("Target workspace:", cfg.workspace_id)
state = setup.provision(cfg, setup.Tokens(notebook_utils=notebookutils))
```

This cell **does contact Fabric and creates items if missing**. It doesn't geocode
or ingest data. It prints resource IDs/URIs, not tokens. Save these identifiers in
your private run notes if desired. It handles synchronous and asynchronous item
creation, paginated item discovery, endpoint readiness, and the correct KQL parent
relationship. Missing child databases are explicitly created after a short
discovery interval; no undocumented auto-created child is assumed.

By default, the three names match the original tutorial. Change all relevant names
in `Config` before provisioning if necessary and substitute them when following
the tutorial. Don't change names, sample files, timestamps, or speed settings
between retries against the same populated resources.

### 3. Manually attach the lakehouse (required)

In **Data items**, add the `UtilityOutagesLakehouse` lakehouse identified by
`state["lakehouse_id"]`, and **set it as the default lakehouse**. Verify that it is
schema-enabled and has the **dbo** schema. Restart the Spark session when Fabric
requests it. This manual attachment is required; provisioning alone doesn't
attach a lakehouse to a notebook.

After a restart, rerun the extraction/import cell and the configuration/provisioning
cell. Setup-owned items are reused, not recreated. The attachment survives; Python
variables don't. Phase 2 checks workspace and default-lakehouse IDs and refuses to
write if they don't match. For a reused lakehouse created outside this package,
schema support is a manual prerequisite: this code doesn't migrate lakehouses.

### 4. Authenticate to Maps and prepare all data

In a separate cell, configure your approved public-client application's IDs:

```python
from azure.identity import DeviceCodeCredential

maps_credential = DeviceCodeCredential(
    tenant_id="<MICROSOFT_ENTRA_TENANT_ID>",
    client_id="<APP_REGISTRATION_APPLICATION_CLIENT_ID>",
)
try:
    report = setup.prepare_data(
        cfg, state, spark=spark, notebook_utils=notebookutils,
        maps_credential=maps_credential,
    )
finally:
    maps_credential.close()
```

Complete the interactive sign-in when prompted. The access token is obtained for
the Maps scope, not Fabric or Kusto. The script never prints or saves it. If your
organization instead exposes an approved managed identity to `DefaultAzureCredential`,
you can omit `maps_credential` (assign that identity Maps Data Reader first). Fabric
notebooks aren't assumed to expose an Azure managed identity automatically.

Phase 2 does the following:

1. Checks package hashes, resource identity, default-lakehouse attachment, existing
   table schemas/data, and conflicting context files.
2. Builds all 105 raw rows with **fixed simulated timestamps**:
   `report_start_utc + EventOffsetSeconds / speed_multiplier`. Defaults start at
   `2026-10-21T12:00:00Z` and compress the original 2,700-second span to 270 seconds.
   There is no waiting/replay and timestamps are **not the execution clock**.
   This intentionally approximates a 10x tutorial simulator run deterministically,
   not an exact reproduction of network delays. Use these sample times for map
   date filters. Setting `speed_multiplier=1` preserves the CSV time span and can
   change group memberships. Configure this only before first ingestion.
3. Geocodes each normalized unique address with Azure Maps **Get Geocoding
   2026-01-01**, Seattle bounding-box bias, `top=1`, finite-coordinate checks, and
   the tutorial's **High confidence + Good match code + Address type** acceptance.
   Other matches keep `Review`/`NoMatch`/`Error` with null coordinates. Nothing is
   fabricated to force 105 map points. Cache is per run, with a complete durable
   enrichment checkpoint before ingestion; it isn't a production cache/queue.
4. Uses the documented Kusto **ingest-from-query** management command with explicit
   ordered `datatable` types to write the small tutorial seed. `ingest-by` tags,
   `ingestIfNotExists`, content checks, and no automatic mutation retry prevent
   blind duplicate appends. This is a small, synchronous demo ingestion—not a
   queued/high-volume producer. It doesn't invent a Fabric ingestion REST API.
5. Creates `CurrentOutageConditions()` only if absent; an existing function is
   reused only when its body/parameters agree with the packaged definition.
6. Reads persisted enriched data through the Fabric Kusto Spark connector, writes
   `dbo.incidenthistory` with Delta merge, and runs a second same-source merge with
   row-count/content/unique-ID checks. It uses Spark, **not** a read-only SQL
   analytics endpoint or a nonexistent REST table-write endpoint.
7. Copies four original GeoJSON files and creates two derived files in
   `Files/OperationalContext`. It prints status totals, accepted point count, actual
   probable-group/comparison counts, and context feature counts.

Keep the notebook session running until the cell finishes. A cell interruption
isn't a transaction rollback across services. Read the recovery guidance before
retrying. Expected raw/enriched/history counts are 105; accepted map points and
probable-group counts depend on actual Azure Maps responses and aren't promised.
No live geocode or group-count baseline is bundled.

## Verify and continue with the map tutorial

1. Inspect the printed report. All status counts must total 105. The script refuses
   to report map handoff readiness if there are no accepted points or groups.
2. Open the provisioned KQL database, create/open a KQL query if needed, and run the
   queries in `verify-handoff.kql` **separately**. Raw and enriched totals must be
   105 unique incidents; known polygons must be `OUT-201`–`OUT-203`.
3. In the lakehouse SQL analytics endpoint, run `verify-history.sql`. Both history
   counts should be 105. Newly written Delta metadata/data can take time to appear;
   refresh/wait if Spark passed but SQL hasn't caught up. **Don't reingest data to
   resolve SQL visibility lag.** The SQL endpoint is read-only for these tables.
4. Start the original tutorial at **Create a utility outage monitoring map**.
   Create the map manually. At **Add the current incidents layer**, run the
   tutorial's stored `CurrentIncidents()` command. Add its layer and the prepared
   `CurrentOutageConditions()` layer exactly as documented.
5. At **Add operational context from the lakehouse**, skip uploading/converting
   files: all six GeoJSON outputs already exist in `Files/OperationalContext`.
   Begin with adding the lakehouse connection to the map. Continue styling,
   clustering, heatmap, tooltips, filters, and crew review as described.

Skip the tutorial's earlier simulator, Eventstream, geocoding notebook, ingestion,
analysis-function creation, and persistence setup steps. Those workflows are not
running in parallel with this preparation. A `UtilityOutageAnalysis` queryset isn't
required by the functions; create a query manually if following a later step that
uses that name. Optional dashboards/alerts and UI configuration remain manual.

Analysis limitations are unchanged: “current” means latest enrichment, with no
rolling lookback or resolved-status exclusion; all known polygons are treated as
active context. Groups need 3 incidents and 2 distinct addresses in the same S2
cell/time bin; points are S2 centers. Incident ID lists cap at 100. The score is a
tutorial heuristic, not a safety standard or proof of shared outage cause. Static
crew context isn't tracking or automated dispatch. No incident neighborhood
classification is added to the enriched schema.

## Existing resources and reruns

- Use a dedicated workspace and **one execution at a time**. This sample has no
  distributed lock and doesn't claim exactly-once behavior under concurrent runs,
  external writes, extent deletion, or retention changes.
- Items created by this setup are identified by their exact description marker.
  Existing foreign items with the same name cause a stop. Set
  `reuse_existing_items=True` only after deliberately choosing those tutorial
  resources; it authorizes creation of missing tutorial tables/functions/files,
  not replacement of other content. Shared production resources aren't suitable.
- KQL table schema/order must match exactly. Existing populated raw/known tables
  must match the configured sample completely; partial, extra, duplicate, or
  different data causes a stop instead of append, delete, or schema alteration.
  All 105 existing enriched rows must match the raw fields and geocoding contract.
  No second geocoding or ingestion is performed on a successful rerun.
- History must be Delta with the exact schema. Identical or empty/partial matching
  history can be completed by merge; unrelated IDs or conflicting rows are refused.
  This conservative demo doesn't overwrite existing newer/older snapshots. The
  merge condition matches the tutorial, but production incremental updates need a
  separate owner-controlled workflow.
- Original context is compared semantically and never overwritten. Generated
  context is also create-if-absent/compare, deliberately safer than the tutorial's
  replace-on-rerun cell. A changed file requires deliberate manual review.
- HTTP read/geocoding retries are bounded with exponential backoff and
  `Retry-After`. Resource-creation and Kusto management POSTs aren't automatically
  retried when acceptance might be ambiguous. Rerun the same phases to discover
  existing items and verify saved data. If accepted ingestion is visible, it is
  reused; differing/partial data is never “repaired” with a broad append/deletion.
- A complete lakehouse enrichment checkpoint lets a retry resume without new Maps
  requests when ingestion hasn't yet occurred. An interruption during geocoding
  before the complete checkpoint may require repeating those requests. Restrict
  access to the checkpoint; it contains addresses. No checkpoint is distributed
  in the downloadable package.
- Failed geocodes are retained, not automatically retried on successful reruns.
  Correcting/re-enriching them is outside this bounded setup. If most/all are
  `Error`, check Maps sign-in/RBAC/account Client ID/network access before a new
  isolated run. Don't manually erase rows from shared tables. Keep failed-run
  resources for diagnosis until you decide what to clean up.
- A failure doesn't roll back items already created. Record their IDs and inspect
  them in Fabric. There are **no delete/drop/truncate/replace operations** in the
  setup and no blanket cleanup command. Delete only dedicated tutorial items that
  you personally verified, through the Fabric UI; stop the notebook session to
  release capacity. Keep your Azure Maps account if it is used elsewhere.

## Optional local provisioning and offline tests

Phase 1 can run in local Python 3.10+ with `DefaultAzureCredential` and an existing
approved developer/managed-identity sign-in. It acquires Fabric tokens for
`https://api.fabric.microsoft.com/.default` and Power BI tokens only when polling
trusted `*.analysis.windows.net` LRO URLs. No credential is embedded or logged.
Local provisioning doesn't replace the required Fabric PySpark/default-lakehouse
phase. The CLI prints the IDs; phase 1 can be rerun in the notebook to rediscover
them without copying state files or credentials.

From the extracted `maps-first-setup` directory:

```text
python -m pip install -r requirements.txt
python setup_utility_outages.py verify
python -m unittest discover -s tests -v
python setup_utility_outages.py provision --workspace-id <WORKSPACE_ID>
```

Only the last command contacts cloud services. Don't run it as part of offline
validation. It doesn't invoke an Azure CLI command or modify your sign-in context.

To rebuild a verified archive from the checked-in files:

```text
python package_setup.py --print-manifest
python package_setup.py
```

The first command prints the manifest to review/update after intentional source
changes. The second checks the existing manifest, packages only an explicit file
allowlist, checks for common credential patterns/path leaks, verifies each archived
hash, and refuses to overwrite any existing ZIP. Neither contacts a cloud service.
The manifest excludes itself to avoid a recursive checksum.

## Validation status and references

Offline tests exercise real local parsing, the 105-row/source contracts, context
conversion, geocode acceptance/failure handling, retry/audience/host guards,
mocked Fabric LRO/pagination, idempotent ingestion guards, runtime attachment,
and package integrity. Syntax is checked separately. **They don't prove live
Fabric provisioning, Entra consent, Azure Maps results, Spark connector/Delta
execution, SQL synchronization, or map rendering.** Run the interactive workflow
in an isolated workspace before treating it as live-validated. Confirm actual
group counts and both comparison outcomes during that walkthrough; the source
tutorial also contains pending UI validation notes.

The existing static/real-time REST tutorial Python examples were reviewed for
centralized configuration, token helpers, `httpx`, LRO polling, and step functions.
No standalone Python companion was present in the inspected Maps sample folders;
the reference implementations were the Python code in the articles. This package
uses the same broad style but doesn't copy their map/eventstream automation or
unsafe schema-merging behavior.

- [Original utility outage tutorial](https://learn.microsoft.com/fabric/real-time-intelligence/map/tutorial-location-aware-utility-outage-monitoring-map)
- [Static Maps REST/Python tutorial](https://learn.microsoft.com/fabric/real-time-intelligence/map/tutorial-create-fabric-map-python)
- [Real-time Maps REST/Python tutorial](https://learn.microsoft.com/fabric/real-time-intelligence/map/tutorial-create-real-time-map-python)
- [Deploy an eventhouse using Fabric APIs](https://learn.microsoft.com/fabric/real-time-intelligence/eventhouse-deploy-with-fabric-api)
- [Get Eventhouse](https://learn.microsoft.com/rest/api/fabric/eventhouse/items/get-eventhouse)
- [Get KQL Database](https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/get-kql-database)
- [Create KQL Database](https://learn.microsoft.com/rest/api/fabric/kqldatabase/items/create-kql-database)
- [Create Lakehouse](https://learn.microsoft.com/rest/api/fabric/lakehouse/items/create-lakehouse)
- [Schema-enabled lakehouses](https://learn.microsoft.com/fabric/data-engineering/lakehouse-schemas#create-a-lakehouse-with-schemas-using-the-rest-api)
- [Fabric LRO protocol](https://learn.microsoft.com/rest/api/fabric/articles/long-running-operation)
- [Kusto ingest-from-query and ingestIfNotExists](https://learn.microsoft.com/kusto/management/data-ingestion/ingest-from-query?view=microsoft-fabric)
- [Kusto table schema inspection](https://learn.microsoft.com/kusto/management/show-table-schema-command?view=microsoft-fabric)
- [Fabric Kusto Spark read/write snippets](https://learn.microsoft.com/fabric/real-time-intelligence/notebooks#use-kusto-snippets-in-a-notebook)
- [NotebookUtils credentials and supported audiences](https://learn.microsoft.com/fabric/data-engineering/notebookutils/notebookutils-credentials)
- [Notebook runtime/default-lakehouse context](https://learn.microsoft.com/fabric/data-engineering/notebookutils/notebookutils-runtime)
- [Azure Maps Get Geocoding](https://learn.microsoft.com/rest/api/maps/search/get-geocoding)
- [Azure Maps geocoding acceptance guidance](https://learn.microsoft.com/azure/azure-maps/how-to-use-best-practices-for-search)
- [Azure Maps authentication](https://learn.microsoft.com/azure/azure-maps/how-to-manage-authentication)
- [DeviceCodeCredential](https://learn.microsoft.com/python/api/azure-identity/azure.identity.devicecodecredential)

The original seven sample files are reused under the repository's MIT license;
their bytes/provenance are preserved in the manifest. They are illustrative
operational context, not authoritative utility boundaries or live outage/crew
data. No new dataset provenance or geocoding-result redistribution rights are
asserted here. Review address privacy and Azure Maps terms before distributing
enriched outputs. Only original inputs, not live geocoded outputs, are packaged.