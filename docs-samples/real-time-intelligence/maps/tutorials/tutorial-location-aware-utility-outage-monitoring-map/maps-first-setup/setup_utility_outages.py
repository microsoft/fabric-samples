"""Bounded, manually invoked setup for the utility outage Maps tutorial.

Importing this module performs no network calls. See README.md before calling
provision() or prepare_data(). No Map, Eventstream, or simulator is created.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin, urlparse


ROOT = Path(__file__).resolve().parent
OWNER = "Utility outage Maps-first setup v1"
FABRIC = "https://api.fabric.microsoft.com/v1/"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
KUSTO_SCOPE = "https://api.kusto.windows.net/.default"
PBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
MAPS_SCOPE = "https://atlas.microsoft.com/.default"
CONNECTOR = "com.microsoft.kusto.spark.synapse.datasource"
RAW_SCHEMA = [
    ("incidentId", "string"), ("reportedAt", "datetime"),
    ("address", "string"), ("incidentType", "string"),
    ("priority", "string"), ("status", "string"),
]
ENRICHED_SCHEMA = RAW_SCHEMA + [
    ("latitude", "real"), ("longitude", "real"),
    ("geocodeStatus", "string"), ("geocodeConfidence", "string"),
    ("geocodeMatchCodes", "string"), ("geocodeResultType", "string"),
    ("matchedAddress", "string"), ("geocodedAt", "datetime"),
]
KNOWN_SCHEMA = [("type", "string"), ("features", "dynamic")]
SAMPLES = (
    "UtilityIncidents.csv", "KnownOutageAreas.geojson",
    "ServiceTerritories.geojson", "UtilityFacilities.geojson", "RepairCrews.csv",
    "SeattleNeighborhoodBoundaries.geojson", "NeighborhoodDistrictLookup.csv",
)


@dataclass(frozen=True)
class Config:
    """Nonsecret configuration. Defaults deliberately match the tutorial."""

    workspace_id: str
    sample_dir: Path = ROOT / "data"
    eventhouse_name: str = "UtilityOutageEventhouse"
    database_name: str = "UtilityOutageEventhouse"
    lakehouse_name: str = "UtilityOutagesLakehouse"
    # Fixed simulated UTC times make retry/restart independent of wall-clock time.
    report_start_utc: str = "2026-10-21T12:00:00Z"
    speed_multiplier: float = 10.0
    reuse_existing_items: bool = False
    timeout_seconds: float = 600.0
    azure_maps_client_id: str = ""

    def __post_init__(self):
        uuid.UUID(self.workspace_id)
        if not math.isfinite(self.speed_multiplier) or self.speed_multiplier <= 0:
            raise ValueError("speed_multiplier must be finite and positive.")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be finite and positive.")
        utc(self.report_start_utc)
        for name in (self.eventhouse_name, self.database_name, self.lakehouse_name):
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,122}", name):
                raise ValueError("Use alphanumeric/underscore item names beginning with a letter.")


def utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamps must include a UTC offset.")
    return parsed.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def verify_package(root: Path = ROOT) -> None:
    """Verify shipped files before authenticating; no cloud calls."""
    manifest = read_json(root / "manifest.json")
    for name, expected in manifest["sha256"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Invalid manifest path.")
        actual = hashlib.sha256((root / relative).read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError(f"Package checksum mismatch: {name}")


def load_raw(cfg: Config) -> list[dict[str, Any]]:
    rows = csv_rows(Path(cfg.sample_dir) / "UtilityIncidents.csv")
    required = {"IncidentId", "EventOffsetSeconds", "StreetAddress", "City",
                "StateOrProvince", "PostalCode", "CountryRegion", "IncidentType",
                "Priority", "Status"}
    if len(rows) != 105 or any(not required.issubset(row) for row in rows):
        raise ValueError("Use the complete, original 105-row UtilityIncidents.csv.")
    if len({row["IncidentId"] for row in rows}) != 105:
        raise ValueError("Incident IDs must be unique.")
    start = utc(cfg.report_start_utc)
    result = []
    for row in sorted(rows, key=lambda r: int(r["EventOffsetSeconds"])):
        offset = int(row["EventOffsetSeconds"])
        if offset < 0 or not row["IncidentId"].strip():
            raise ValueError("Invalid source incident ID/offset.")
        result.append({
            "incidentId": row["IncidentId"],
            "reportedAt": iso(start + timedelta(seconds=offset / cfg.speed_multiplier)),
            "address": (f'{row["StreetAddress"]}, {row["City"]}, '
                        f'{row["StateOrProvince"]} {row["PostalCode"]}, {row["CountryRegion"]}'),
            "incidentType": row["IncidentType"], "priority": row["Priority"],
            "status": row["Status"],
        })
    return result


def build_context(sample_dir: Path) -> dict[str, dict]:
    """Preserve original GeoJSON and tutorial property casing."""
    outputs = {}
    for name in SAMPLES:
        if name.endswith(".geojson"):
            collection = read_json(sample_dir / name)
            if collection.get("type") != "FeatureCollection":
                raise ValueError(f"Not a FeatureCollection: {name}")
            outputs[name] = collection
    crews = []
    for row in csv_rows(sample_dir / "RepairCrews.csv"):
        lon, lat = float(row["Longitude"]), float(row["Latitude"])
        if not (-180 <= lon <= 180 and -90 <= lat <= 90):
            raise ValueError("Invalid crew coordinates.")
        crews.append({"type": "Feature", "geometry": {
            "type": "Point", "coordinates": [lon, lat]},
            "properties": {k: v for k, v in row.items() if k not in {"Longitude", "Latitude"}}})
    districts = {row["Neighborhood"]: row["OperatingDistrict"]
                 for row in csv_rows(sample_dir / "NeighborhoodDistrictLookup.csv")}
    neighborhoods = json.loads(canonical(outputs["SeattleNeighborhoodBoundaries.geojson"]))
    for feature in neighborhoods["features"]:
        props = feature["properties"]
        props["OperatingDistrict"] = districts.get(props["Neighborhood"], "Unassigned")
    outputs["RepairCrews.geojson"] = {"type": "FeatureCollection", "features": crews}
    outputs["NeighborhoodDistricts.geojson"] = neighborhoods
    if len(crews) != 7 or len(neighborhoods["features"]) != 9:
        raise ValueError("Expected 7 crews and 9 neighborhoods.")
    known = outputs["KnownOutageAreas.geojson"]["features"]
    if {f["properties"]["OutageId"] for f in known} != {"OUT-201", "OUT-202", "OUT-203"}:
        raise ValueError("Unexpected known-outage sample.")
    return outputs


class Tokens:
    """Notebook tokens or DefaultAzureCredential; never serialize credentials."""

    def __init__(self, notebook_utils=None, credential=None):
        self.utils = notebook_utils
        self.credential = credential

    def get(self, scope: str) -> str:
        if self.utils is not None:
            # Documented audience keys. Don't ask NotebookUtils for an invented Maps key.
            key = "kusto" if scope == KUSTO_SCOPE else "pbi"
            if scope not in {FABRIC_SCOPE, PBI_SCOPE, KUSTO_SCOPE}:
                raise ValueError("Use a separate Entra credential for Azure Maps.")
            return self.utils.credentials.getToken(key)
        if self.credential is None:
            from azure.identity import DefaultAzureCredential
            self.credential = DefaultAzureCredential()
        return self.credential.get_token(scope).token

    def close(self):
        if self.credential is not None:
            self.credential.close()


def safe_url(url: str, plane: str) -> str:
    """Never send bearer tokens to arbitrary hosts supplied by redirects/config."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    allowed = {
        "fabric": host == "api.fabric.microsoft.com" or host.endswith(".analysis.windows.net"),
        "kusto": host.endswith((".kusto.fabric.microsoft.com", ".kusto.data.microsoft.com",
                                   ".kusto.windows.net")),
        "maps": host == "atlas.microsoft.com",
    }
    if (parsed.scheme != "https" or parsed.username or parsed.password
            or parsed.port not in {None, 443} or not allowed.get(plane, False)):
        raise ValueError(f"Untrusted {plane} URL.")
    return url


class ApiError(RuntimeError):
    def __init__(self, status: int, request_id: str = ""):
        # Response bodies and URLs can contain address data; do not log them.
        super().__init__(f"Service returned HTTP {status}; request ID: {request_id or 'unavailable'}")
        self.status = status


class Http:
    def __init__(self, tokens: Tokens, client=None, sleep: Callable = time.sleep):
        import httpx
        self.client = client or httpx.Client(timeout=60, follow_redirects=False)
        self.tokens = tokens
        self.sleep = sleep

    def request(self, method: str, url: str, *, plane="fabric", retry=False,
                headers_factory=None, **kwargs):
        import httpx
        safe_url(url, plane)
        scope = KUSTO_SCOPE if plane == "kusto" else (
            FABRIC_SCOPE if urlparse(url).hostname == "api.fabric.microsoft.com" else PBI_SCOPE)
        for attempt in range(4):
            headers = headers_factory() if headers_factory else {
                "Authorization": f"Bearer {self.tokens.get(scope)}"}
            try:
                response = self.client.request(method, url, headers=headers, **kwargs)
            except httpx.TransportError:
                if not retry or attempt == 3:
                    raise RuntimeError("Transport failure; outcome may be unknown. Rerun to reconcile.") from None
                self.sleep(2 ** attempt)
                continue
            if retry and response.status_code in {429, 500, 502, 503, 504} and attempt < 3:
                self.sleep(retry_after(response, 2 ** attempt))
                continue
            if response.status_code >= 300:
                raise ApiError(response.status_code, response.headers.get("x-ms-request-id", ""))
            return response
        raise RuntimeError("Request attempts exhausted.")

    def close(self):
        self.client.close()
        self.tokens.close()


def retry_after(response, default=2.0) -> float:
    try:
        return max(0.0, min(60.0, float(response.headers.get("Retry-After", default))))
    except ValueError:
        return float(default)


class FabricClient:
    def __init__(self, cfg: Config, http: Http):
        self.cfg, self.http = cfg, http
        self.base = f"{FABRIC}workspaces/{cfg.workspace_id}/"

    def items(self) -> list[dict]:
        url, results, seen = self.base + "items", [], set()
        while url:
            if url in seen:
                raise RuntimeError("Fabric returned a pagination loop.")
            seen.add(url)
            body = self.http.request("GET", url, retry=True).json()
            results.extend(body.get("value", []))
            url = body.get("continuationUri")
        return results

    def get(self, collection: str, item_id: str) -> dict:
        uuid.UUID(item_id)
        return self.http.request("GET", self.base + collection + "/" + item_id, retry=True).json()

    def finish(self, response) -> dict:
        if response.status_code != 202:
            return response.json() if response.content else {}
        operation_id = response.headers.get("x-ms-operation-id")
        url = response.headers.get("Location")
        if not url and operation_id:
            uuid.UUID(operation_id)
            url = FABRIC + "operations/" + operation_id
        if not url:
            raise RuntimeError("Creation accepted without a polling URL; inspect the workspace before rerunning.")
        url = urljoin(FABRIC, url)
        deadline = time.monotonic() + self.cfg.timeout_seconds
        while time.monotonic() < deadline:
            self.http.sleep(retry_after(response))
            response = self.http.request("GET", url, retry=True)
            body = response.json() if response.content else {}
            if body.get("id"):
                return body
            state = str(body.get("status", "")).casefold()
            if state in {"failed", "cancelled", "canceled"}:
                raise RuntimeError("Fabric item creation failed. Inspect its operation in Fabric.")
            if state == "succeeded":
                # Resolve created items from the paginated workspace inventory. Some
                # services return status-only results, so /result isn't assumed.
                return {}
            if state not in {"notstarted", "running", "undefined", ""}:
                raise RuntimeError("Unrecognized Fabric operation state.")
            next_url = response.headers.get("Location")
            if next_url:
                url = urljoin(url, next_url)
        raise TimeoutError("Provisioning timeout; do not repost blindly. Rerun to reconcile item inventory.")

    def ensure_item(self, kind: str, collection: str, name: str, payload=None) -> dict:
        matches = [i for i in self.items() if i.get("type") == kind and i["displayName"] == name]
        if len(matches) > 1:
            raise RuntimeError(f"Ambiguous {kind} name: {name}")
        if matches:
            item = self.get(collection, matches[0]["id"])
            if item.get("description") != OWNER and not self.cfg.reuse_existing_items:
                raise RuntimeError(f"Existing {kind} is not setup-owned: {name}. Use an isolated workspace or explicitly opt into reuse.")
            return item
        body = {"displayName": name, "description": OWNER, **(payload or {})}
        # Never retry a POST automatically: acceptance may have occurred before a timeout.
        result = self.finish(self.http.request("POST", self.base + collection, json=body))
        deadline = time.monotonic() + self.cfg.timeout_seconds
        while time.monotonic() < deadline:
            matches = [i for i in self.items() if i.get("type") == kind and i["displayName"] == name]
            if len(matches) > 1:
                raise RuntimeError(f"Ambiguous newly created {kind}: {name}")
            if matches:
                if result.get("id") and result["id"] != matches[0]["id"]:
                    raise RuntimeError("Concurrent item creation detected.")
                return self.get(collection, matches[0]["id"])
            self.http.sleep(2)
        raise TimeoutError(f"{kind} not visible after creation. Inspect workspace before rerunning.")


def provision(cfg: Config, tokens: Tokens | None = None) -> dict:
    """Phase 1: create/reuse three item types, discover actual resource properties."""
    verify_package()
    load_raw(cfg)
    build_context(Path(cfg.sample_dir))
    http = Http(tokens or Tokens())
    try:
        client = FabricClient(cfg, http)
        eventhouse = client.ensure_item("Eventhouse", "eventhouses", cfg.eventhouse_name)
        # Eventhouse creation can supply a default child database. Discover it first.
        deadline = time.monotonic() + min(30, cfg.timeout_seconds)
        while True:
            eventhouse = client.get("eventhouses", eventhouse["id"])
            child_ids = eventhouse.get("properties", {}).get("databasesItemIds", [])
            children = [client.get("kqlDatabases", child_id) for child_id in child_ids]
            matches = [c for c in children if c["displayName"] == cfg.database_name]
            if len(matches) > 1:
                raise RuntimeError("Ambiguous child KQL databases.")
            if matches:
                database = matches[0]
                break
            if child_ids or time.monotonic() >= deadline:
                database = client.ensure_item("KQLDatabase", "kqlDatabases", cfg.database_name, {
                    "creationPayload": {"databaseType": "ReadWrite",
                                        "parentEventhouseItemId": eventhouse["id"]}})
                break
            # Allow automatically provisioned children time to appear before creating one.
            http.sleep(5)
        deadline = time.monotonic() + cfg.timeout_seconds
        while True:
            database = client.get("kqlDatabases", database["id"])
            props = database.get("properties", {})
            if props.get("parentEventhouseItemId") != eventhouse["id"]:
                raise RuntimeError("KQL database belongs to a different eventhouse; refusing reuse.")
            if props.get("databaseType") != "ReadWrite":
                raise RuntimeError("A writable KQL database is required.")
            if props.get("queryServiceUri") and props.get("ingestionServiceUri"):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("KQL database endpoints are not ready.")
            http.sleep(3)
        safe_url(props["queryServiceUri"], "kusto")
        safe_url(props["ingestionServiceUri"], "kusto")
        lakehouse = client.ensure_item("Lakehouse", "lakehouses", cfg.lakehouse_name, {
            "creationPayload": {"enableSchemas": True}})
        state = {"workspace_id": cfg.workspace_id, "eventhouse_id": eventhouse["id"],
                 "database_id": database["id"], "database_name": database["displayName"],
                 "query_uri": props["queryServiceUri"],
                 "ingestion_uri": props["ingestionServiceUri"], "lakehouse_id": lakehouse["id"]}
        print("Provisioning complete. Attach this lakehouse as the notebook default before phase 2:")
        print(json.dumps(state, indent=2))  # Resource IDs only, never credentials.
        return state
    finally:
        http.close()


def kql_name(name: str) -> str:
    return "[" + json.dumps(name) + "]"


def kql_value(value: Any, kind: str) -> str:
    if kind == "string":
        return json.dumps(value or "", ensure_ascii=True)
    if value is None:
        return f"{kind}(null)"
    if kind == "datetime":
        return "datetime(" + iso(utc(str(value))) + ")"
    if kind == "dynamic":
        return "dynamic(" + canonical(value) + ")"
    if kind == "real":
        if isinstance(value, bool) or not math.isfinite(float(value)):
            raise ValueError("Nonfinite/non-numeric coordinate.")
        return "real(" + str(float(value)) + ")"
    raise ValueError("Unsupported KQL type.")


def datatable(rows: list[dict], schema: list[tuple[str, str]]) -> str:
    columns = ", ".join(f"{name}:{kind}" for name, kind in schema)
    values = ",\n".join(", ".join(kql_value(row.get(name), kind) for name, kind in schema) for row in rows)
    return f"datatable({columns})[\n{values}\n]"


def normalize_rows(rows: list[dict], schema: list[tuple[str, str]]) -> list[str]:
    """Canonical comparison handles Kusto null strings and timestamp formatting."""
    normalized = []
    for row in rows:
        values = {}
        for name, kind in schema:
            value = row.get(name)
            if kind == "datetime" and value is not None:
                value = iso(utc(str(value)))
            elif kind == "string":
                value = value or ""
            elif kind == "dynamic" and isinstance(value, str):
                value = json.loads(value)
            values[name] = value
        normalized.append(canonical(values))
    return sorted(normalized)


class Kusto:
    def __init__(self, http: Http, state: dict):
        self.http, self.state = http, state

    def wait_ready(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            try:
                self.execute("print Ready=1")
                return
            except ApiError as error:
                if error.status not in {404, 409, 429, 500, 502, 503, 504}:
                    raise
                if time.monotonic() >= deadline:
                    raise TimeoutError("KQL data plane is not ready.") from None
                self.http.sleep(3)

    def execute(self, text: str, management=False) -> list[dict]:
        path = "/v1/rest/mgmt" if management else "/v1/rest/query"
        response = self.http.request("POST", self.state["query_uri"].rstrip("/") + path,
                                     plane="kusto", retry=not management,
                                     json={"db": self.state["database_name"], "csl": text})
        body = response.json()
        if body.get("error") or body.get("Exceptions"):
            raise RuntimeError("Kusto returned an execution error; inspect the query in Fabric.")
        tables = body.get("Tables", [])
        for table in tables:
            if table.get("TableName") in {"QueryStatus", "QueryCompletionInformation"}:
                columns = [c["ColumnName"] for c in table.get("Columns", [])]
                for row in table.get("Rows", []):
                    status = dict(zip(columns, row))
                    severity = status.get("Severity")
                    if ((isinstance(severity, (int, float)) and severity <= 2)
                            or any(str(cell).casefold() == "error" for cell in row)):
                        raise RuntimeError("Kusto returned a partial query failure.")
        primary = next((t for t in tables if t.get("TableName") == "PrimaryResult"), None)
        if primary is None:
            primary = next((t for t in tables if t.get("TableName") not in {
                "QueryStatus", "QueryCompletionInformation", "TableOfContents"}), None)
        if primary is None:
            raise RuntimeError("Kusto response has no result table.")
        names = [column["ColumnName"] for column in primary["Columns"]]
        return [dict(zip(names, row)) for row in primary["Rows"]]

    def table(self, name: str, schema: list[tuple[str, str]]) -> list[dict]:
        existing = self.execute(".show tables", management=True)
        if name not in {r["TableName"] for r in existing}:
            columns = ", ".join(f"{n}:{t}" for n, t in schema)
            self.execute(f".create table {kql_name(name)} ({columns})", management=True)
        details = self.execute(f".show table {kql_name(name)} schema as json", management=True)
        actual = json.loads(details[0]["Schema"])["OrderedColumns"]
        actual = [(c["Name"], c["CslType"]) for c in actual]
        if actual != schema:
            raise RuntimeError(f"Existing {name} schema differs; no schema changes were made.")
        # This is a small tutorial dataset. Bound reads and reject larger/shared tables.
        rows = self.execute(f"{kql_name(name)} | take 107")
        if len(rows) > 105:
            raise RuntimeError(f"Unexpected shared/duplicate data in {name}.")
        return rows

    def ingest_once(self, name: str, rows: list[dict], schema: list[tuple[str, str]]) -> None:
        existing = self.table(name, schema)
        if existing:
            if normalize_rows(existing, schema) != normalize_rows(rows, schema):
                raise RuntimeError(f"Existing {name} differs or is partial; refusing to append or replace.")
            print(f"Reused {name}: {len(existing)} rows (no ingestion).")
            return
        tag = "utility-maps-v1-" + digest([name, rows])
        command = (f".set-or-append {kql_name(name)} with "
                   f"(tags='{canonical(['ingest-by:' + tag])}', "
                   f"ingestIfNotExists='{canonical([tag])}') <|\n{datatable(rows, schema)}")
        # Synchronous ingest-from-query is supported by Fabric. No guessed REST
        # ingestion endpoint, no column-order mismatch, no blind POST retries.
        self.execute(command, management=True)
        saved = self.table(name, schema)
        if normalize_rows(saved, schema) != normalize_rows(rows, schema):
            raise RuntimeError(f"Post-ingestion verification failed for {name}. Stop and inspect; do not append.")
        print(f"Ingested and verified {name}: {len(rows)} rows.")

    def ensure_function(self, command: str) -> None:
        functions = self.execute(".show functions", management=True)
        matches = [f for f in functions if f["Name"] == "CurrentOutageConditions"]
        body = command[command.index("{"):].strip()
        if matches:
            existing = matches[0]
            # Whitespace differences are harmless, but don't overwrite different logic.
            if (compact_kql(existing["Body"]) != compact_kql(body)
                    or str(existing.get("Parameters", "()")).strip() not in {"()", ""}):
                raise RuntimeError("Existing CurrentOutageConditions differs; refusing to overwrite.")
            print("Reused identical CurrentOutageConditions().")
            return
        self.execute(command, management=True)


def compact_kql(text: str) -> str:
    # Preserve whitespace inside literals: changing a category string changes logic.
    return re.sub(r'"(?:\\.|[^"\\])*"|\s+',
                  lambda match: match[0] if match[0].startswith('"') else "", text)


def classify_geocode(payload: dict) -> dict:
    result = {"latitude": None, "longitude": None, "geocodeStatus": "NoMatch",
              "geocodeConfidence": None, "geocodeMatchCodes": None,
              "geocodeResultType": None, "matchedAddress": None}
    features = payload.get("features", [])
    if not features:
        return result
    feature = features[0]
    props = feature.get("properties", {})
    codes = props.get("matchCodes") or []
    coords = feature.get("geometry", {}).get("coordinates", [])
    valid = (len(coords) >= 2 and all(isinstance(c, (int, float)) and not isinstance(c, bool)
             and math.isfinite(c) for c in coords[:2])
             and -180 <= coords[0] <= 180 and -90 <= coords[1] <= 90)
    accepted = (props.get("confidence") == "High" and "Good" in codes
                and props.get("type") == "Address" and valid)
    result.update({"geocodeStatus": "Matched" if accepted else "Review",
                   "geocodeConfidence": props.get("confidence"),
                   "geocodeMatchCodes": ",".join(codes), "geocodeResultType": props.get("type"),
                   "matchedAddress": (props.get("address") or {}).get("formattedAddress")})
    if accepted:
        result.update(longitude=float(coords[0]), latitude=float(coords[1]))
    return result


def geocode_all(raw: list[dict], cfg: Config, http: Http, credential) -> list[dict]:
    if not cfg.azure_maps_client_id:
        raise ValueError("Set azure_maps_client_id to the Azure Maps account's client ID (not the app registration ID).")
    uuid.UUID(cfg.azure_maps_client_id)
    cache, enriched = {}, []

    def headers():
        return {"Authorization": f"Bearer {credential.get_token(MAPS_SCOPE).token}",
                "x-ms-client-id": cfg.azure_maps_client_id}

    for incident in raw:
        key = " ".join(incident["address"].casefold().split())
        if key not in cache:
            try:
                response = http.request("GET", "https://atlas.microsoft.com/geocode", plane="maps",
                    retry=True, headers_factory=headers, params={"api-version": "2026-01-01",
                    "query": incident["address"], "bbox": "-122.4597,47.4810,-122.2244,47.7341", "top": 1})
                cache[key] = classify_geocode(response.json())
            except (ApiError, RuntimeError, ValueError):
                # All source incidents are retained, even after exhausted service retries.
                cache[key] = {**classify_geocode({}), "geocodeStatus": "Error"}
        enriched.append({**incident, **cache[key], "geocodedAt": iso(datetime.now(timezone.utc))})
    print(f"Geocoding: {len(cache)} unique addresses; statuses: {dict(Counter(r['geocodeStatus'] for r in enriched))}")
    return enriched


def validate_enriched(rows: list[dict], raw: list[dict]) -> None:
    if len(rows) != 105 or len({r["incidentId"] for r in rows}) != 105:
        raise ValueError("Enriched data must contain all 105 unique incidents, including failed geocodes.")
    if normalize_rows(rows, RAW_SCHEMA) != normalize_rows(raw, RAW_SCHEMA):
        raise ValueError("Enriched source fields differ from this configured sample run.")
    for row in rows:
        utc(row["geocodedAt"])
        if row["geocodeStatus"] not in {"Matched", "Review", "NoMatch", "Error"}:
            raise ValueError("Unknown geocoding status.")
        if row["geocodeStatus"] == "Matched":
            lon, lat = row["longitude"], row["latitude"]
            if (not isinstance(lon, (int, float)) or not isinstance(lat, (int, float))
                    or not (-180 <= lon <= 180 and -90 <= lat <= 90)
                    or row.get("geocodeConfidence") != "High"
                    or "Good" not in (row.get("geocodeMatchCodes") or "").split(",")
                    or row.get("geocodeResultType") != "Address"):
                raise ValueError("Invalid accepted geocode.")
        elif row.get("longitude") is not None or row.get("latitude") is not None:
            raise ValueError("Unaccepted geocodes must not contain coordinates.")


def write_once(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise RuntimeError(f"Existing output differs; refusing to overwrite {path.name}.")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(content)


def check_runtime(cfg: Config, state: dict, utils) -> None:
    context = utils.runtime.context
    expected = {"currentWorkspaceId": cfg.workspace_id,
                "defaultLakehouseWorkspaceId": cfg.workspace_id,
                "defaultLakehouseId": state["lakehouse_id"]}
    if state["workspace_id"] != cfg.workspace_id:
        raise ValueError("Provisioning state belongs to a different workspace.")
    for key, value in expected.items():
        if str(context.get(key, "")).casefold() != str(value).casefold():
            raise RuntimeError("Attach the provisioned lakehouse as the notebook default in the target workspace, then restart the Spark session.")
    if not Path("/lakehouse/default/Files").is_dir():
        raise RuntimeError("Default lakehouse Files mount is unavailable.")


def persist_history(spark, rows: list[dict]) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql import types as T

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    types = {"string": T.StringType(), "real": T.DoubleType(), "datetime": T.TimestampType()}
    schema = T.StructType([T.StructField(n, types[t], True) for n, t in ENRICHED_SCHEMA])
    source_rows = [{n: (utc(r[n]).replace(tzinfo=None) if t == "datetime" else r.get(n))
                    for n, t in ENRICHED_SCHEMA} for r in rows]
    source = spark.createDataFrame(source_rows, schema).cache()
    target = "dbo.incidenthistory"
    try:
        if not spark.catalog.tableExists(target):
            source.limit(0).write.format("delta").mode("errorifexists").saveAsTable(target)
        saved = spark.table(target)
        if [(f.name, f.dataType.simpleString()) for f in saved.schema] != [
                (f.name, f.dataType.simpleString()) for f in schema]:
            raise RuntimeError("Existing dbo.incidenthistory schema differs; no schema migration attempted.")
        if not DeltaTable.isDeltaTable(spark, spark.sql(f"DESCRIBE DETAIL {target}").first()["location"]):
            raise RuntimeError("incidenthistory is not a Delta table.")
        if saved.count() != saved.select("incidentId").distinct().count():
            raise RuntimeError("Existing history has duplicate/null incident IDs.")
        if saved.select("incidentId").join(source.select("incidentId"), "incidentId", "left_anti").limit(1).count():
            raise RuntimeError("History contains unrelated incidents; refusing to write a shared table.")
        # Don't silently replace conflicting existing incident snapshots.
        if saved.count() and (saved.exceptAll(source).limit(1).count()):
            raise RuntimeError("Existing history differs from source; review it instead of overwriting.")
        def merge():
            (DeltaTable.forName(spark, target).alias("target")
             .merge(source.alias("source"), "target.incidentId = source.incidentId")
             .whenMatchedUpdateAll(condition="source.geocodedAt > target.geocodedAt")
             .whenNotMatchedInsertAll().execute())
        merge()
        before = spark.table(target)
        if before.count() != 105 or before.exceptAll(source).limit(1).count():
            raise RuntimeError("History count/content verification failed.")
        merge()  # The tutorial's same-source duplicate-prevention test.
        after = spark.table(target)
        if (after.count() != 105 or after.select("incidentId").distinct().count() != 105
                or after.exceptAll(source).limit(1).count()):
            raise RuntimeError("History same-source rerun verification failed.")
        print("dbo.incidenthistory: 105 rows/IDs; second upsert passed (all statuses retained).")
    finally:
        source.unpersist()


def prepare_data(cfg: Config, state: dict, *, spark, notebook_utils, maps_credential=None) -> dict:
    """Phase 2: run only in an interactive Fabric PySpark notebook, single writer."""
    verify_package()
    check_runtime(cfg, state, notebook_utils)
    raw = load_raw(cfg)
    context = build_context(Path(cfg.sample_dir))
    known = context["KnownOutageAreas.geojson"]
    http = Http(Tokens(notebook_utils=notebook_utils))
    owned_credential = None
    try:
        # Re-read API properties: never trust a hand-edited state URI/database string.
        client = FabricClient(cfg, http)
        db = client.get("kqlDatabases", state["database_id"])
        lakehouse = client.get("lakehouses", state["lakehouse_id"])
        props = db.get("properties", {})
        if (props.get("parentEventhouseItemId") != state["eventhouse_id"]
                or props.get("databaseType") != "ReadWrite"
                or db["displayName"] != cfg.database_name
                or lakehouse["displayName"] != cfg.lakehouse_name):
            raise RuntimeError("Provisioned resource identities no longer match configuration.")
        state = {**state, "query_uri": props["queryServiceUri"],
                 "ingestion_uri": props["ingestionServiceUri"], "database_name": db["displayName"]}
        safe_url(state["query_uri"], "kusto")
        kusto = Kusto(http, state)
        kusto.wait_ready(cfg.timeout_seconds)
        # Verify all table schemas before any ingestion. Existing rows must agree.
        existing_raw = kusto.table("RawIncidents", RAW_SCHEMA)
        existing_enriched = kusto.table("EnrichedIncidents", ENRICHED_SCHEMA)
        existing_known = kusto.table("KnownOutageAreas.geojson", KNOWN_SCHEMA)
        for name, existing, expected, schema in [
                ("RawIncidents", existing_raw, raw, RAW_SCHEMA),
                ("KnownOutageAreas.geojson", existing_known, [known], KNOWN_SCHEMA)]:
            if existing and normalize_rows(existing, schema) != normalize_rows(expected, schema):
                raise RuntimeError(f"Existing {name} differs; refusing duplicate/partial ingestion.")
        if existing_enriched:
            validate_enriched(existing_enriched, raw)
        # Context uses create-if-absent, not overwrite. Compare every destination first.
        context_dir = Path("/lakehouse/default/Files/OperationalContext")
        for name, collection in context.items():
            destination = context_dir / name
            if destination.exists() and read_json(destination) != collection:
                raise RuntimeError(f"Existing context differs: {name}. No context files were overwritten.")
        checkpoint = Path("/lakehouse/default/Files/UtilityOutageSetup/enriched-checkpoint.json")
        fingerprint = digest([raw, cfg.azure_maps_client_id, "2026-01-01", "High/Good/Address"])
        if existing_enriched:
            enriched = existing_enriched
            print("Reusing 105 validated enriched incidents; Azure Maps won't be called.")
        elif checkpoint.exists():
            saved = read_json(checkpoint)
            if saved["source_fingerprint"] != fingerprint:
                raise RuntimeError("Checkpoint belongs to different inputs/configuration; refusing reuse.")
            enriched = saved["rows"]
            validate_enriched(enriched, raw)
            print("Resuming validated enrichment checkpoint; Azure Maps won't be called.")
        else:
            if maps_credential is None:
                from azure.identity import DefaultAzureCredential
                owned_credential = DefaultAzureCredential()
                maps_credential = owned_credential
            # Fail before sending addresses when Entra authentication is unavailable.
            maps_credential.get_token(MAPS_SCOPE)
            enriched = geocode_all(raw, cfg, http, maps_credential)
            validate_enriched(enriched, raw)
            write_once(checkpoint, canonical({"source_fingerprint": fingerprint, "rows": enriched}).encode())
        kusto.ingest_once("RawIncidents", raw, RAW_SCHEMA)
        kusto.ingest_once("EnrichedIncidents", enriched, ENRICHED_SCHEMA)
        kusto.ingest_once("KnownOutageAreas.geojson", [known], KNOWN_SCHEMA)
        command = (ROOT / "CurrentOutageConditions.kql").read_text(encoding="utf-8")
        kusto.ensure_function(command)
        # Read saved eventhouse data through the supported Fabric Spark connector.
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        source = (spark.read.format(CONNECTOR)
                  .option("accessToken", notebook_utils.credentials.getToken("kusto"))
                  .option("kustoCluster", state["query_uri"])
                  .option("kustoDatabase", state["database_name"])
                  .option("kustoQuery", "EnrichedIncidents | summarize arg_max(geocodedAt, *) by incidentId")
                  .load())
        persisted_rows = [r.asDict(recursive=True) for r in source.collect()]
        for row in persisted_rows:
            for field in ("reportedAt", "geocodedAt"):
                value = row[field]
                if isinstance(value, datetime):
                    row[field] = iso(value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value)
        validate_enriched(persisted_rows, raw)
        persist_history(spark, persisted_rows)
        for name, collection in context.items():
            destination = context_dir / name
            if not destination.exists():
                # Original GeoJSON files are copied byte-for-byte; generated ones are JSON.
                original = Path(cfg.sample_dir) / name
                content = original.read_bytes() if original.is_file() else canonical(collection).encode()
                write_once(destination, content)
        groups = kusto.execute("CurrentOutageConditions()")
        accepted = sum(row["geocodeStatus"] == "Matched" for row in enriched)
        counts = dict(Counter(row["geocodeStatus"] for row in enriched))
        report = {"state": state, "incident_count": 105, "geocode_status_counts": counts,
                  "accepted_map_points": accepted, "probable_outage_groups": len(groups),
                  "comparison_counts": dict(Counter(g["OutageComparison"] for g in groups)),
                  "context_feature_counts": {n: len(c["features"]) for n, c in context.items()},
                  "maps_created": 0, "continuous_processing": False}
        print(json.dumps(report, indent=2))
        if not accepted or not groups:
            raise RuntimeError("Data retained, but no accepted map points or probable groups. Review geocoding/authentication; map handoff is not ready.")
        print("Ready for the tutorial's Create a utility outage monitoring map section. Create CurrentIncidents() there.")
        return report
    finally:
        if owned_credential is not None:
            owned_credential.close()
        http.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["verify", "provision"])
    parser.add_argument("--workspace-id", default=os.environ.get("FABRIC_WORKSPACE_ID", ""))
    parser.add_argument("--reuse-existing-items", action="store_true")
    args = parser.parse_args()
    if args.action == "verify":
        verify_package()
        cfg = Config(workspace_id="00000000-0000-0000-0000-000000000000")
        print(f"Offline verification passed: {len(load_raw(cfg))} incidents; {len(build_context(cfg.sample_dir))} context outputs.")
    else:
        provision(Config(workspace_id=args.workspace_id, reuse_existing_items=args.reuse_existing_items))


if __name__ == "__main__":
    main()