"""Offline only: no credential discovery, Spark session, or real network calls."""

import ast
import copy
import hashlib
import json
import math
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import setup_utility_outages as s
import package_setup as packaging

WORKSPACE = "11111111-1111-1111-1111-111111111111"
EVENTHOUSE = "22222222-2222-2222-2222-222222222222"
DATABASE = "33333333-3333-3333-3333-333333333333"
LAKEHOUSE = "44444444-4444-4444-4444-444444444444"
MAPS_ACCOUNT = "55555555-5555-5555-5555-555555555555"
URI = "https://test.z0.kusto.fabric.microsoft.com"
STATE = {"workspace_id": WORKSPACE, "eventhouse_id": EVENTHOUSE,
         "database_id": DATABASE, "database_name": "UtilityOutageEventhouse",
         "query_uri": URI, "ingestion_uri": URI, "lakehouse_id": LAKEHOUSE}


def cfg(**kwargs):
    return s.Config(workspace_id=WORKSPACE, **kwargs)


def response(status=200, body=None, headers=None):
    return httpx.Response(status, json=body if body is not None else {}, headers=headers or {})


def feature(**overrides):
    result = {"features": [{"geometry": {"type": "Point", "coordinates": [-122.32, 47.62]},
               "properties": {"confidence": "High", "matchCodes": ["Good"], "type": "Address",
                              "address": {"formattedAddress": "Example address"}}}]}
    result["features"][0]["properties"].update(overrides)
    return result


def enriched(raw=None):
    raw = raw or s.load_raw(cfg())
    return [{**row, **s.classify_geocode(feature()), "geocodedAt": "2026-10-21T13:00:00Z"}
            for row in raw]


class DataTests(unittest.TestCase):
    def test_complete_deterministic_raw_sample(self):
        rows = s.load_raw(cfg())
        self.assertEqual(len(rows), 105)
        self.assertEqual(len({r["incidentId"] for r in rows}), 105)
        self.assertEqual(rows, s.load_raw(cfg()))
        self.assertEqual(set(rows[0]), {name for name, _ in s.RAW_SCHEMA})
        self.assertEqual(rows[0]["reportedAt"], "2026-10-21T12:00:00Z")
        span = s.utc(rows[-1]["reportedAt"]) - s.utc(rows[0]["reportedAt"])
        self.assertEqual(span.total_seconds(), 270)
        self.assertNotIn("latitude", rows[0])

    def test_original_time_span(self):
        rows = s.load_raw(cfg(speed_multiplier=1))
        self.assertEqual((s.utc(rows[-1]["reportedAt"]) - s.utc(rows[0]["reportedAt"])).total_seconds(), 2700)

    def test_invalid_configuration(self):
        for speed in (0, -1, math.nan, math.inf):
            with self.subTest(speed=speed), self.assertRaises(ValueError):
                cfg(speed_multiplier=speed)
        with self.assertRaises(ValueError):
            cfg(report_start_utc="2026-10-21T12:00:00")
        with self.assertRaises(ValueError):
            cfg(lakehouse_name="../invalid")

    def test_context_counts_properties_and_original_integrity(self):
        outputs = s.build_context(cfg().sample_dir)
        self.assertEqual(len(outputs), 6)
        self.assertEqual(len(outputs["RepairCrews.geojson"]["features"]), 7)
        self.assertEqual(len(outputs["NeighborhoodDistricts.geojson"]["features"]), 9)
        crew = outputs["RepairCrews.geojson"]["features"][0]
        self.assertEqual(crew["geometry"]["coordinates"], [-122.384, 47.668])
        self.assertIn("AvailabilityStatus", crew["properties"])
        self.assertNotIn("Latitude", crew["properties"])
        self.assertEqual({f["properties"]["OperatingDistrict"]
                          for f in outputs["NeighborhoodDistricts.geojson"]["features"]},
                         {"North", "Central", "South", "West"})
        original = s.read_json(cfg().sample_dir / "SeattleNeighborhoodBoundaries.geojson")
        self.assertTrue(all("OperatingDistrict" not in f["properties"] for f in original["features"]))
        self.assertEqual(s.build_context(cfg().sample_dir), outputs)

    def test_all_geocode_statuses_preserved(self):
        rows = enriched()
        for index, status in enumerate(("Review", "NoMatch", "Error")):
            rows[index].update(geocodeStatus=status, latitude=None, longitude=None)
        s.validate_enriched(rows, s.load_raw(cfg()))
        self.assertEqual(len(rows), 105)
        rows[0]["latitude"] = 47.0
        with self.assertRaisesRegex(ValueError, "Unaccepted"):
            s.validate_enriched(rows, s.load_raw(cfg()))

    def test_partial_duplicate_or_changed_enrichment_refused(self):
        raw = s.load_raw(cfg())
        for rows in (enriched(raw)[:-1], enriched(raw) + enriched(raw)[:1]):
            with self.assertRaises(ValueError):
                s.validate_enriched(rows, raw)
        rows = enriched(raw)
        rows[0]["address"] = "changed"
        with self.assertRaises(ValueError):
            s.validate_enriched(rows, raw)

    def test_geocode_acceptance_requires_all_three_signals(self):
        accepted = s.classify_geocode(feature())
        self.assertEqual(accepted["geocodeStatus"], "Matched")
        self.assertEqual(accepted["longitude"], -122.32)
        self.assertEqual(accepted["latitude"], 47.62)
        for change in ({"confidence": "Medium"}, {"matchCodes": ["Ambiguous"]}, {"type": "RoadBlock"}):
            with self.subTest(change=change):
                review = s.classify_geocode(feature(**change))
                self.assertEqual(review["geocodeStatus"], "Review")
                self.assertIsNone(review["latitude"])
                self.assertIsNone(review["longitude"])
        self.assertEqual(s.classify_geocode({"features": []})["geocodeStatus"], "NoMatch")

    def test_invalid_candidate_coordinates_not_accepted(self):
        for coordinates in ([math.nan, 47], [-200, 47], [], [True, 47], [0], [0, 100]):
            payload = feature()
            payload["features"][0]["geometry"]["coordinates"] = coordinates
            result = s.classify_geocode(payload)
            self.assertEqual(result["geocodeStatus"], "Review")
            self.assertIsNone(result["longitude"])

    def test_kql_order_escaping_nulls_and_dynamic(self):
        row = {"incidentId": 'INC-"a"\\b', "reportedAt": "2026-10-21T12:00:00Z"}
        query = s.datatable([row], s.RAW_SCHEMA)
        self.assertTrue(query.startswith("datatable(incidentId:string, reportedAt:datetime"))
        self.assertIn('INC-\\"a\\"\\\\b', query)
        self.assertEqual(s.kql_value(None, "real"), "real(null)")
        self.assertEqual(s.kql_value(None, "string"), '""')
        self.assertTrue(s.kql_value({"features": []}, "dynamic").startswith("dynamic("))
        with self.assertRaises(ValueError):
            s.kql_value(math.nan, "real")

    def test_normalized_service_roundtrip(self):
        rows = enriched()
        copy_rows = copy.deepcopy(rows)
        copy_rows[0]["reportedAt"] = "2026-10-21T12:00:00.0000000Z"
        rows[0]["matchedAddress"], copy_rows[0]["matchedAddress"] = None, ""
        self.assertEqual(s.normalize_rows(rows, s.ENRICHED_SCHEMA),
                         s.normalize_rows(copy_rows, s.ENRICHED_SCHEMA))

    def test_create_if_absent_file_safety(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "data.json"
            s.write_once(path, b"first")
            s.write_once(path, b"first")
            with self.assertRaises(RuntimeError):
                s.write_once(path, b"different")
            self.assertEqual(path.read_bytes(), b"first")


class HttpTests(unittest.TestCase):
    def make_http(self, handler):
        tokens = Mock()
        tokens.get.return_value = "offline-token"
        sleeps = []
        http = s.Http(tokens, client=httpx.Client(transport=httpx.MockTransport(handler)), sleep=sleeps.append)
        self.addCleanup(http.close)
        return http, tokens, sleeps

    def test_correct_audiences_and_fresh_tokens(self):
        http, tokens, _ = self.make_http(lambda request: response())
        for url, plane in [(s.FABRIC + "items", "fabric"),
                           ("https://df-test.analysis.windows.net/operation", "fabric"),
                           (URI + "/v1/rest/query", "kusto")]:
            http.request("GET", url, plane=plane)
        self.assertEqual([c.args[0] for c in tokens.get.call_args_list],
                         [s.FABRIC_SCOPE, s.PBI_SCOPE, s.KUSTO_SCOPE])

    def test_untrusted_hosts_never_receive_tokens(self):
        http, tokens, _ = self.make_http(lambda request: self.fail("Network request not allowed"))
        for url in ("http://api.fabric.microsoft.com/v1/items", "https://api.fabric.microsoft.com.evil.test",
                    "https://evil.test", "https://user:password@api.fabric.microsoft.com/v1",
                    "https://api.fabric.microsoft.com:8443/v1"):
            with self.assertRaises(ValueError):
                http.request("GET", url)
        tokens.get.assert_not_called()

    def test_read_throttling_retry_after(self):
        replies = iter([response(429, headers={"Retry-After": "3"}), response()])
        http, _, sleeps = self.make_http(lambda request: next(replies))
        http.request("GET", s.FABRIC + "items", retry=True)
        self.assertEqual(sleeps, [3.0])

    def test_mutation_not_retried_and_error_redacted(self):
        seen = []
        def handler(request):
            seen.append(request)
            return response(503, {"secret": "must-not-print"})
        http, _, sleeps = self.make_http(handler)
        with self.assertRaises(s.ApiError) as caught:
            http.request("POST", s.FABRIC + "items", json={"test": 1})
        self.assertEqual(len(seen), 1)
        self.assertFalse(sleeps)
        self.assertNotIn("must-not-print", str(caught.exception))

    def test_notebook_token_keys(self):
        utils = SimpleNamespace(credentials=SimpleNamespace(getToken=Mock(return_value="offline")))
        tokens = s.Tokens(notebook_utils=utils)
        tokens.get(s.FABRIC_SCOPE)
        tokens.get(s.KUSTO_SCOPE)
        self.assertEqual([c.args[0] for c in utils.credentials.getToken.call_args_list], ["pbi", "kusto"])
        with self.assertRaises(ValueError):
            tokens.get(s.MAPS_SCOPE)

    def test_geocoding_cache_and_real_request_contract(self):
        seen = []
        def handler(request):
            seen.append(request)
            self.assertEqual(request.url.path, "/geocode")
            self.assertEqual(request.url.params["api-version"], "2026-01-01")
            self.assertEqual(request.headers["x-ms-client-id"], MAPS_ACCOUNT)
            self.assertEqual(request.headers["Authorization"], "Bearer maps-offline-token")
            return response(body=feature())
        http, _, _ = self.make_http(handler)
        credential = Mock()
        credential.get_token.return_value = SimpleNamespace(token="maps-offline-token")
        rows = s.load_raw(cfg())[:2]
        rows[1]["address"] = "  " + rows[0]["address"].upper() + "  "
        result = s.geocode_all(rows, cfg(azure_maps_client_id=MAPS_ACCOUNT), http, credential)
        self.assertEqual(len(seen), 1)
        self.assertEqual(len(result), 2)
        credential.get_token.assert_called_with(s.MAPS_SCOPE)

    def test_exhausted_maps_errors_retain_every_incident(self):
        http, _, _ = self.make_http(lambda request: response(503))
        credential = Mock()
        credential.get_token.return_value = SimpleNamespace(token="offline")
        raw = s.load_raw(cfg())[:3]
        result = s.geocode_all(raw, cfg(azure_maps_client_id=MAPS_ACCOUNT), http, credential)
        self.assertEqual(len(result), 3)
        self.assertTrue(all(row["geocodeStatus"] == "Error" for row in result))


class FabricTests(unittest.TestCase):
    def client(self, replies):
        http = Mock()
        http.request.side_effect = replies
        return s.FabricClient(cfg(), http), http

    def test_paginated_item_discovery(self):
        next_url = s.FABRIC + "workspaces/" + WORKSPACE + "/items?continuationToken=two"
        client, http = self.client([response(body={"value": [{"id": "one"}], "continuationUri": next_url}),
                                   response(body={"value": [{"id": "two"}]})])
        self.assertEqual([i["id"] for i in client.items()], ["one", "two"])
        self.assertEqual(http.request.call_args_list[-1].args[1], next_url)

    def test_pagination_loop_refused(self):
        url = s.FABRIC + "workspaces/" + WORKSPACE + "/items"
        client, _ = self.client([response(body={"value": [], "continuationUri": url})])
        with self.assertRaises(RuntimeError):
            client.items()

    def test_lro_running_then_succeeded_status_only(self):
        client, http = self.client([response(body={"status": "Running"}),
                                   response(body={"status": "Succeeded"})])
        self.assertEqual(client.finish(response(202, headers={"x-ms-operation-id": DATABASE})), {})
        self.assertEqual(http.request.call_count, 2)

    def test_lro_failure_and_missing_poll_url(self):
        client, _ = self.client([response(body={"status": "Failed"})])
        with self.assertRaises(RuntimeError):
            client.finish(response(202, headers={"Location": s.FABRIC + "operations/" + DATABASE}))
        with self.assertRaises(RuntimeError):
            client.finish(response(202))

    def test_synchronous_creation_result(self):
        client, http = self.client([])
        self.assertEqual(client.finish(response(201, {"id": DATABASE})), {"id": DATABASE})
        http.request.assert_not_called()

    def test_foreign_item_is_not_silently_reused(self):
        item = {"id": LAKEHOUSE, "displayName": "UtilityOutagesLakehouse", "type": "Lakehouse"}
        client, _ = self.client([response(body={"value": [item]}), response(body=item)])
        with self.assertRaisesRegex(RuntimeError, "not setup-owned"):
            client.ensure_item("Lakehouse", "lakehouses", "UtilityOutagesLakehouse")

    def test_owned_item_is_reused_without_post(self):
        item = {"id": LAKEHOUSE, "displayName": "UtilityOutagesLakehouse", "type": "Lakehouse",
                "description": s.OWNER}
        client, http = self.client([response(body={"value": [item]}), response(body=item)])
        self.assertEqual(client.ensure_item("Lakehouse", "lakehouses", "UtilityOutagesLakehouse"), item)
        self.assertTrue(all(call.args[0] == "GET" for call in http.request.call_args_list))

    def test_duplicate_item_names_refused(self):
        item = {"id": LAKEHOUSE, "displayName": "UtilityOutagesLakehouse", "type": "Lakehouse"}
        client, _ = self.client([response(body={"value": [item, item]})])
        with self.assertRaisesRegex(RuntimeError, "Ambiguous"):
            client.ensure_item("Lakehouse", "lakehouses", "UtilityOutagesLakehouse")

    def test_wrong_default_lakehouse_fails_before_writes(self):
        utils = SimpleNamespace(runtime=SimpleNamespace(context={
            "currentWorkspaceId": WORKSPACE, "defaultLakehouseWorkspaceId": WORKSPACE,
            "defaultLakehouseId": DATABASE}))
        with self.assertRaisesRegex(RuntimeError, "default"):
            s.check_runtime(cfg(), STATE, utils)


class KustoTests(unittest.TestCase):
    def kusto(self):
        return s.Kusto(Mock(), STATE)

    def test_identical_data_is_not_reingested(self):
        kusto, rows = self.kusto(), s.load_raw(cfg())
        kusto.table = Mock(return_value=rows)
        kusto.execute = Mock()
        kusto.ingest_once("RawIncidents", rows, s.RAW_SCHEMA)
        kusto.execute.assert_not_called()

    def test_partial_data_refused_without_append(self):
        kusto, rows = self.kusto(), s.load_raw(cfg())
        kusto.table = Mock(return_value=rows[:10])
        kusto.execute = Mock()
        with self.assertRaisesRegex(RuntimeError, "partial"):
            kusto.ingest_once("RawIncidents", rows, s.RAW_SCHEMA)
        kusto.execute.assert_not_called()

    def test_empty_table_ingestion_has_tags_and_postcheck(self):
        kusto, rows = self.kusto(), s.load_raw(cfg())
        kusto.table = Mock(side_effect=[[], rows])
        kusto.execute = Mock()
        kusto.ingest_once("RawIncidents", rows, s.RAW_SCHEMA)
        command = kusto.execute.call_args.args[0]
        self.assertIn(".set-or-append", command)
        self.assertIn("ingestIfNotExists", command)
        self.assertIn("ingest-by:", command)
        self.assertIn("datatable(incidentId:string, reportedAt:datetime", command)
        self.assertTrue(kusto.execute.call_args.kwargs["management"])

    def test_schema_order_mismatch_is_not_merged(self):
        kusto = self.kusto()
        schema = {"OrderedColumns": [{"Name": n, "CslType": t} for n, t in reversed(s.RAW_SCHEMA)]}
        kusto.execute = Mock(side_effect=[[{"TableName": "RawIncidents"}], [{"Schema": json.dumps(schema)}]])
        with self.assertRaisesRegex(RuntimeError, "schema differs"):
            kusto.table("RawIncidents", s.RAW_SCHEMA)
        self.assertFalse(any(".create" in c.args[0] for c in kusto.execute.call_args_list))

    def test_identical_function_reused_and_conflicting_logic_refused(self):
        kusto = self.kusto()
        command = (s.ROOT / "CurrentOutageConditions.kql").read_text(encoding="utf-8")
        body = command[command.index("{"):].strip()
        kusto.execute = Mock(return_value=[{"Name": "CurrentOutageConditions", "Body": body, "Parameters": "()"}])
        kusto.ensure_function(command)
        self.assertEqual(kusto.execute.call_count, 1)
        kusto.execute.return_value[0]["Body"] = body.replace('"Potential new outage"', '"Potentialnewoutage"')
        with self.assertRaisesRegex(RuntimeError, "differs"):
            kusto.ensure_function(command)

    def test_partial_query_failure_numeric_severity(self):
        kusto = self.kusto()
        kusto.http.request.return_value = response(body={"Tables": [
            {"TableName": "PrimaryResult", "Columns": [{"ColumnName": "x"}], "Rows": [[1]]},
            {"TableName": "QueryStatus", "Columns": [{"ColumnName": "Severity"}], "Rows": [[2]]}]})
        with self.assertRaisesRegex(RuntimeError, "partial query failure"):
            kusto.execute("print x=1")


class PackageTests(unittest.TestCase):
    def test_all_sources_parse(self):
        for name in packaging.FILES:
            if name.endswith(".py"):
                ast.parse((s.ROOT / name).read_text(encoding="utf-8"), filename=name, feature_version=(3, 10))

    def test_manifest_matches_allowlist(self):
        s.verify_package()
        self.assertEqual(s.read_json(s.ROOT / "manifest.json"), packaging.make_manifest())

    def test_archive_contents_and_exclusive_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            target = packaging.build(Path(directory) / "test.zip")
            with zipfile.ZipFile(target) as archive:
                names = archive.namelist()
                self.assertEqual(len(names), len(packaging.FILES) + 1)
                self.assertTrue(all(n.startswith("maps-first-setup/") for n in names))
                self.assertFalse(any("__pycache__" in n or n.endswith("-test.csv") for n in names))
            with self.assertRaises(FileExistsError):
                packaging.build(target)

    def test_common_credential_patterns_blocked(self):
        # Assemble synthetic markers so the shipped test source doesn't itself
        # resemble a private-key file to the archive's safety scanner.
        for content in (b"-----BEGIN " + b"PRIVATE KEY-----", b"SharedAccessKey=" + b"a" * 30):
            with self.assertRaises(ValueError):
                packaging.check_content(content, "test")

    def test_analysis_contract_and_no_map_creation(self):
        command = (s.ROOT / "CurrentOutageConditions.kql").read_text(encoding="utf-8")
        for fragment in ("let TimeWindow = 15m", "let S2Level = 14", "MinimumIncidents = 3",
                         "DistinctAddresses >= 2", "make_set(incidentId, 100)",
                         "geo_point_in_polygon", "PriorityScore >= 20", "PriorityScore >= 12"):
            self.assertIn(fragment, command)
        self.assertNotIn(".create-or-alter function CurrentIncidents", command)
        source = (s.ROOT / "setup_utility_outages.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        # Fail if future code adds map/eventstream item creation to this setup.
        for call in (n for n in ast.walk(tree) if isinstance(n, ast.Call)):
            if isinstance(call.func, ast.Attribute) and call.func.attr == "ensure_item":
                self.assertIn(call.args[0].value, {"Eventhouse", "KQLDatabase", "Lakehouse"})


if __name__ == "__main__":
    unittest.main()