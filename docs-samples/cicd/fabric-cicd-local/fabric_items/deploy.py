"""
Deploy the Fabric items in this folder to a workspace.

Prerequisites:
1. pip install -r requirements.txt
2. az login                          # authenticates the AzureCliCredential below
3. Replace workspace_id with your target workspace's GUID

Run with: python deploy.py
"""

from pathlib import Path
from fabric_cicd import FabricWorkspace, publish_all_items  # 👈 import the function
from azure.identity import AzureCliCredential  # 👈 requires an active `az login` session

repo_dir = Path(__file__).resolve().parent  # ...\fabric_items

workspace = FabricWorkspace(
    workspace_id="<YOUR_WORKSPACE_ID>",
    repository_directory=str(repo_dir),
    token_credential=AzureCliCredential(),
    # environment="DEV",  # optional, but required if you use parameter replacement via parameter.yml
    # item_type_in_scope=["Notebook", "DataPipeline", "Environment"],  # optional scope
)

publish_all_items(workspace)  # 👈 call the function

