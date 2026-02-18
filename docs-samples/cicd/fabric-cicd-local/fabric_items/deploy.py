from pathlib import Path
from fabric_cicd import FabricWorkspace, publish_all_items  # 👈 import the function

repo_dir = Path(__file__).resolve().parent  # ...\fabric_items

workspace = FabricWorkspace(
    workspace_id="<YOUR_WORKSPACE_ID>",
    repository_directory=str(repo_dir),
    # environment="DEV",  # optional, but required if you use parameter replacement via parameter.yml
    # item_type_in_scope=["Notebook", "DataPipeline", "Environment"],  # optional scope
)

publish_all_items(workspace)  # 👈 call the function

