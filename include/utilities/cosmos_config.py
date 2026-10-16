import os
import shutil
from pathlib import Path

from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode
from cosmos.profiles import (
    RedshiftUserPasswordProfileMapping,
)

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt" / "asset_correlation_redshift"
DBT_EXECUTABLE = os.getenv(
    "DBT_EXECUTABLE_PATH", shutil.which("dbt") or "/usr/local/bin/dbt"
)

project_config = ProjectConfig(DBT_PROJECT_DIR)

profile_config = ProfileConfig(
    profile_name="asset_correlation_redshift",
    target_name=os.getenv("DBT_TARGET", "dev"),
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_default",
        profile_args={"schema": os.getenv("DBT_TARGET_SCHEMA", "analytics")},
    ),
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS,
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,  # tukar ke DBT_MANIFEST untuk production
    dbt_executable_path=DBT_EXECUTABLE,
    invocation_mode=InvocationMode.SUBPROCESS,
)
