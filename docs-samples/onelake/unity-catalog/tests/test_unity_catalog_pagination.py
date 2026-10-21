import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests


# Fabric's notebookutils module is not available outside a notebook.
notebookutils = types.ModuleType("notebookutils")
notebookutils.mssparkutils = Mock()
spec = importlib.util.spec_from_file_location(
    "unity_catalog_util", Path(__file__).resolve().parents[1] / "util.py"
)
util = importlib.util.module_from_spec(spec)
with patch.dict(sys.modules, {"notebookutils": notebookutils}):
    spec.loader.exec_module(util)

CONFIG = {
    "dbx_workspace": "https://example.cloud.databricks.com/",
    "dbx_token": "test-token",
    "dbx_uc_catalog": "main",
    "dbx_uc_schemas": ["sales", "inventory"],
}


def response(tables=None, next_token=None):
    body = {}
    if tables is not None:
        body["tables"] = tables
    if next_token is not None:
        body["next_page_token"] = next_token
    result = Mock()
    result.json.return_value = body
    return result


class PaginationTests(unittest.TestCase):
    @patch.object(util.requests, "get")
    def test_reads_all_pages_and_resets_token_for_each_schema(self, get):
        requests_seen = []
        pages = iter([
            response([{"name": "a"}], "p1"),
            response(None, "p2"),  # An empty page need not be the last page.
            response([{"name": "b"}]),
            response([{"name": "c"}]),
        ])

        def fetch(url, *, params, headers, timeout):
            requests_seen.append(dict(params))
            self.assertEqual(url, "https://example.cloud.databricks.com/api/2.1/unity-catalog/tables")
            self.assertEqual(headers["Authorization"], "Bearer test-token")
            self.assertEqual(timeout, 30)
            return next(pages)

        get.side_effect = fetch
        tables = util.Utils.get_dbx_uc_tables(CONFIG)
        self.assertEqual([item["name"] for item in tables], ["a", "b", "c"])
        self.assertEqual(
            requests_seen,
            [
                {"catalog_name": "main", "schema_name": "sales", "max_results": 0},
                {"catalog_name": "main", "schema_name": "sales", "max_results": 0, "page_token": "p1"},
                {"catalog_name": "main", "schema_name": "sales", "max_results": 0, "page_token": "p2"},
                {"catalog_name": "main", "schema_name": "inventory", "max_results": 0},
            ],
        )

    @patch.object(util.requests, "get")
    def test_empty_final_token_ends_pagination(self, get):
        get.side_effect = [response([{"name": "a"}], "p1"), response([], ""), response([])]
        self.assertEqual(
            util.Utils.get_dbx_uc_tables(CONFIG), [{"name": "a"}]
        )
        self.assertEqual(get.call_count, 3)

    @patch.object(util.requests, "get")
    def test_repeated_token_is_rejected(self, get):
        get.side_effect = [response([], "loop"), response([], "loop")]
        with self.assertRaisesRegex(ValueError, "pagination token"):
            util.Utils.get_dbx_uc_tables(CONFIG)

    @patch.object(util.requests, "get")
    def test_bad_page_is_rejected(self, get):
        get.return_value = response()
        get.return_value.json.return_value = {"tables": "wrong-type"}
        with self.assertRaisesRegex(ValueError, "Unexpected Unity Catalog response"):
            util.Utils.get_dbx_uc_tables(CONFIG)

    @patch.object(util.requests, "get")
    def test_failed_later_page_never_touches_shortcuts(self, get):
        failed = response()
        failed.raise_for_status.side_effect = requests.HTTPError("403")
        get.side_effect = [response([{"name": "a"}], "p1"), failed]
        with patch.object(util.Utils, "get_lakehouse_shortcuts", create=True) as ls, \
             patch.object(util.Utils, "delete_shortcuts", create=True) as delete, \
             patch.object(util.Utils, "create_shortcuts", create=True) as create:
            with self.assertRaises(requests.HTTPError):
                util.sync_dbx_uc_tables_to_onelake(
                    CONFIG, {"consider_dbx_uc_table_changes": True}
                )
            ls.assert_not_called()
            delete.assert_not_called()
            create.assert_not_called()

    def test_missing_schemas_does_not_list_the_entire_catalog(self):
        with self.assertRaisesRegex(ValueError, "At least one"):
            util.Utils.get_dbx_uc_tables({**CONFIG, "dbx_uc_schemas": []})


if __name__ == "__main__":
    unittest.main()
