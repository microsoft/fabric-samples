import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor
from notebookutils import mssparkutils

class Utils:

    FABRIC_API_ENDPOINT = "api.fabric.microsoft.com"
    ONELAKE_API_ENDPOINT = "onelake.dfs.fabric.microsoft.com"
    PREFIX = "uc"

    # DBX utils

    @staticmethod
    def get_dbx_uc_tables(databricks_config):
        all_tables = []

        dbx_workspace = databricks_config['dbx_workspace']
        dbx_token = databricks_config['dbx_token']
        dbx_uc_catalog = databricks_config['dbx_uc_catalog']
        dbx_uc_schemas = databricks_config['dbx_uc_schemas']
  
        for schema in dbx_uc_schemas:
            url = f"{dbx_workspace}/api/2.1/unity-catalog/tables?catalog_name={dbx_uc_catalog}&schema_name={schema}"
            payload = {}
            headers = {
                'Authorization': f'Bearer {dbx_token}',
                'Content-Type': 'application/json'
            }
            response = requests.get(url, data=payload, headers=headers)

            if response.status_code == 200:
                response_json = json.loads(response.text)
                all_tables.extend(response_json['tables'])
            else:
                print(f"! Upps [{response.status_code}] Cannot connect to Unity Catalog. Please review configs.")
                return None
        return all_tables

    # Fabric utils

    def get_lakehouse_shortcuts(fabric_config):
        class FileInfo:
            def __init__(self, path, name, size):
                self.path = path
                self.name = name
                self.size = size

        workspace_id = fabric_config['workspace_id']
        lakehouse_id = fabric_config['lakehouse_id']

        table_infos = mssparkutils.fs.ls(f"abfss://{workspace_id}@{Utils.ONELAKE_API_ENDPOINT}/{lakehouse_id}/Tables")
        current_shortcut_names = [table_info.name for table_info in table_infos]
        return current_shortcut_names
    
    @staticmethod
    def delete_shortcuts(fabric_config, deletions_required):
        sc_deleted, sc_failed = 0, 0
        workspace_id = fabric_config['workspace_id']
        lakehouse_id = fabric_config['lakehouse_id']

        for table_name in deletions_required:
            url = f"https://{Utils.FABRIC_API_ENDPOINT}/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts/Tables/{table_name}"
            token = mssparkutils.credentials.getToken('pbi')

            payload = {}
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
            }
            
            response = requests.delete(url, json=payload, headers=headers)

            if response.status_code == 200:
                print(f"∟ Shortcut deleted successfully with name:'{table_name}'")
                sc_deleted += 1
            elif response.status_code == 429:
                retry_after_seconds = 60
                if 'Retry-After' in response.headers:
                    retry_after_seconds = int(response.headers['Retry-After']) + 5
                print(f"! Upps [429] Exceeded the amount of calls while deleting '{table_name}', sleeping for {retry_after_seconds} seconds.")
                time.sleep(retry_after_seconds)
                sc_failed += 1
            elif response.status_code != 404:
                print(f"! Upps [{response.status_code}] Failed to delete shortcut '{table_name}'. Response details: {response.text}")
                sc_failed += 1

        return sc_deleted, sc_failed 

    @staticmethod
    def create_shortcuts(fabric_config, tables):
        sc_created, sc_failed, sc_skipped = 0, 0, 0

        workspace_id = fabric_config['workspace_id']
        lakehouse_id = fabric_config['lakehouse_id']
        shortcut_connection_id = fabric_config['shortcut_connection_id']
        #shortcut_name = fabric_config['shortcut_name']
        skip_if_shortcut_exists = True #fabric_config['skip_if_shortcut_exists']

        max_retries = 3
        max_threads = 2

        def create_shortcut(table):
            nonlocal sc_created, sc_failed, sc_skipped
            catalog_name = table['catalog_name']
            schema_name = table['schema_name']
            operation = table['operation']
            #table_name = shortcut_name.format(schema=schema_name, table=table['name'], catalog=catalog_name)
            table_name = f"{Utils.PREFIX}_{catalog_name}_{schema_name}_{table['name']}"
            table_type = table['table_type']

            if operation == "create":

                if table_type in {"EXTERNAL"}:

                    data_source_format = table['data_source_format']
                    table_location = table['storage_location']

                    if data_source_format == "DELTA":
                        # Remove the 'abfss://' scheme from the path
                        without_scheme = table_location.replace("abfss://", "", 1)

                        # Extract the storage account name and the rest of the path
                        container_end = without_scheme.find("@")
                        container = without_scheme[:container_end]
                        remainder = without_scheme[container_end + 1:]

                        account_end = remainder.find("/")
                        storage_account = remainder[:account_end]
                        path = remainder[account_end + 1:]
                        https_path = f"https://{storage_account}/{container}"

                        url = f"https://{Utils.FABRIC_API_ENDPOINT}/v1/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"

                        if not skip_if_shortcut_exists:
                            url += "?shortcutConflictPolicy=GenerateUniqueName"

                        token = mssparkutils.credentials.getToken('pbi')

                        payload = {
                            "path": "Tables",
                            "name": table_name,
                            "target": {
                                "adlsGen2": {
                                    "location": https_path,
                                    "subpath": path,
                                    "connectionId": shortcut_connection_id
                                }
                            }
                        }
                        headers = {
                            'Authorization': f'Bearer {token}',
                            'Content-Type': 'application/json',
                        }

                        for attempt in range(max_retries):
                            try:
                                response = requests.post(url, json=payload, headers=headers)
                                if response.status_code == 429:
                                    retry_after_seconds = 60
                                    if 'Retry-After' in response.headers:
                                        retry_after_seconds = int(response.headers['Retry-After']) + 5
                                    print(f"! Upps [429] Exceeded the amount of calls while creating '{table_name}', sleeping for {retry_after_seconds} seconds.")
                                    time.sleep(retry_after_seconds)
                                elif response.status_code in [200, 201]:
                                    data = json.loads(response.text)
                                    print(f"∟ Shortcut created successfully with name:'{data['name']}'")
                                    sc_created += 1
                                    break
                                elif response.status_code == 400:
                                    data = json.loads(response.text)
                                    error_details = data.get('moreDetails', [])
                                    error_message = error_details[0].get('message', 'No error message found')
                                    if error_message == "Copy, Rename or Update of shortcuts are not supported by OneLake.":
                                        print(f"∟ Skipped shortcut creation for '{table_name}'. Shortcut with the SAME NAME exists.")
                                        sc_skipped += 1
                                        break
                                    elif "Unauthorized. Access to target location" in error_message:
                                        print(f"! Upps [400] Cannot create shortcut for '{table_name}'. Access denied, please review.")
                                    else:
                                        print(f"! Upps [{response.status_code}] Failed to create shortcut. Response details: {response.text}")
                                elif response.status_code == 403:
                                    print(f"! Upps [403] Cannot create shortcut for '{table_name}'. Access forbidden, please review.")
                                else:
                                    print(f"! Upps [{response.status_code}] Failed to create shortcut '{table_name}'. Response details: {response.text}")
                            except requests.RequestException as e:
                                print(f"Request failed: {e}")
                            if attempt < 2:
                                sleep_time = 2 ** attempt
                                print(f"___ Retrying in {sleep_time} seconds for '{table_name}'...")
                                time.sleep(sleep_time)
                            else:
                                print(f"! Max retries reached for '{table_name}'. Exiting.")
                                sc_failed += 1
                                break
                    else:
                        print(f"∟ Skipped shortcut creation for '{table_name}'. Format not supported: {data_source_format}.")
                        sc_skipped += 1
                else:
                    print(f"∟ Skipped shortcut creation for '{table_name}'. Table type is not EXTERNAL.")
                    sc_skipped += 1
            else:
                print(f"∟ Skipped shortcut creation for '{table_name}'. Shortcut with the SAME NAME exists.")
                sc_skipped += 1 
            
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            executor.map(create_shortcut, tables)
        
        return sc_created, sc_skipped, sc_failed


def sync_dbx_uc_tables_to_onelake(databricks_config, fabric_config):
    tables = Utils.get_dbx_uc_tables(databricks_config)
    all_shortcuts = Utils.get_lakehouse_shortcuts(fabric_config)

    uc_shortcuts = [name for name in all_shortcuts if name.startswith(Utils.PREFIX)]

    uc_table_names = [f"{Utils.PREFIX}_{entry['catalog_name']}_{entry['schema_name']}_{entry['name']}" for entry in tables]

    skipped_required = [name for name in uc_shortcuts if name in uc_table_names]
    deletions_required = [name for name in uc_shortcuts if name not in uc_table_names]
    creations_required = [name for name in uc_table_names if name not in uc_shortcuts]

    print(f"Started syncing from Unity Catalog to Fabric...")

    # Delete shortcuts if not exist in UC tables
    consider_dbx_uc_table_changes = fabric_config['consider_dbx_uc_table_changes']
    sc_deleted, sc_failed_delete = 0, 0
    if(consider_dbx_uc_table_changes): 
        sc_deleted, sc_failed_delete = Utils.delete_shortcuts(fabric_config, deletions_required)

    # Create shortcuts if not exist in Lakehouse
    if tables is not None:
        for entry in tables:
            if f"{Utils.PREFIX}_{entry['catalog_name']}_{entry['schema_name']}_{entry['name']}" in creations_required:
                entry['operation'] = 'create'
            else:
                entry['operation'] = 'skip'
        sc_created, sc_skipped, sc_failed_create = Utils.create_shortcuts(fabric_config, tables)

    total_failed = sc_failed_delete + sc_failed_create
    print(f"\nSync finished. {sc_created} shortcuts created, {sc_skipped} skipped, {total_failed} failed, {sc_deleted} deleted.")