import os
from dagster import (
    EnvVar,
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)

# from dagster_dbt import DbtCliResource, load_assets_from_dbt_project
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_duckdb import DuckDBResource
from dagster_ssh import SSHResource
from dagstermill import ConfigurableLocalOutputNotebookIOManager

from .oai import cartilage_thickness, run_triggers

from .ingest import ingested_study

from .assets import huggingface

from .resources import (
    # DBT_PROJECT_DIR,
    DATABASE_PATH,
    DATA_DIR,
    CollectionPublisher,
    CollectionTables,
    OAISampler,
    OaiPipelineSSH,
    OaiPipelineSubprocess,
    OaiPipelineSlurm,
    CartilageThicknessTable,
    FileStorage,
)

from .jobs import (
    stage_oai_samples_job,
    stage_oai_sample_job,
    ingest_and_analyze_study_job,
    cartilage_thickness_job,
    cartilage_thickness_oai_job,
)

from .sensors import (
    staged_study_sensor,
    patient_id_sensor,
)

# dbt = DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
duckdb_resource = DuckDBResource(database=DATABASE_PATH)

# dbt_assets = load_assets_from_dbt_project(DBT_PROJECT_DIR, DBT_PROJECT_DIR)
dbt_assets = []
all_assets = load_assets_from_modules(
    [cartilage_thickness, run_triggers, ingested_study, huggingface]
)
all_checks = load_asset_checks_from_modules(
    [cartilage_thickness, run_triggers, ingested_study, huggingface]
)

# Use os.getenv to get the environment variable with a default value
root_dir = os.getenv("FILE_STORAGE_ROOT", str(DATA_DIR))


# pick how to run the OAI pipeline
pipeline_src_dir = EnvVar("PIPELINE_SRC_DIR")
env_setup_command = EnvVar("ENV_SETUP_COMMAND")
oai_pipeline_resource_env = os.getenv("OAI_PIPELINE_RESOURCE", "subprocess")
if oai_pipeline_resource_env == "subprocess":
    oai_pipeline_resource = OaiPipelineSubprocess(
        pipeline_src_dir=pipeline_src_dir,
        env_setup_command=env_setup_command,
    )
elif oai_pipeline_resource_env == "ssh":
    oai_pipeline_resource = OaiPipelineSSH(
        pipeline_src_dir=pipeline_src_dir,
        env_setup_command=env_setup_command,
        ssh_connection=SSHResource(
            remote_host=EnvVar("SSH_HOST"),
            username=EnvVar("SSH_USERNAME"),
            password=EnvVar("SSH_PASSWORD"),
            remote_port=22,
        ),
    )
elif oai_pipeline_resource_env == "slurm":
    oai_pipeline_resource = OaiPipelineSlurm(
        pipeline_src_dir=pipeline_src_dir,
        sbatch_args=EnvVar("SBATCH_ARGS"),
    )
else:
    raise ValueError(
        f"Invalid value for env var OAI_PIPELINE_RESOURCE: {oai_pipeline_resource_env}"
    )


file_storage = FileStorage(root_dir=root_dir)

resources = {
    # "dbt": dbt,
    "io_manager": DuckDBPolarsIOManager(database=DATABASE_PATH, schema="main"),
    "collection_publisher": CollectionPublisher(
        hf_token=EnvVar("HUGGINGFACE_TOKEN"), file_storage=file_storage
    ),
    "duckdb": duckdb_resource,
    "collection_tables": CollectionTables(
        duckdb=duckdb_resource, file_storage=file_storage
    ),
    "oai_sampler": OAISampler(
        oai_data_root=EnvVar("OAI_DATA_ROOT"), file_storage=file_storage
    ),
    "oai_pipeline": oai_pipeline_resource,
    "cartilage_thickness_table": CartilageThicknessTable(
        duckdb=duckdb_resource, file_storage=file_storage
    ),
    "file_storage": file_storage,
    "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
}


defs = Definitions(
    assets=[*dbt_assets, *all_assets],
    asset_checks=all_checks,
    resources=resources,
    jobs=[
        stage_oai_samples_job,
        stage_oai_sample_job,
        ingest_and_analyze_study_job,
        cartilage_thickness_job,
        cartilage_thickness_oai_job,
    ],
    sensors=[
        staged_study_sensor,
        patient_id_sensor,
        run_triggers.oai_study_uids_to_run_sensor,
    ],
)
