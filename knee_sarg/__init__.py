from dagster import EnvVar, Definitions, load_assets_from_modules, define_asset_job

# from dagster_dbt import DbtCliResource, load_assets_from_dbt_project
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_duckdb import DuckDBResource
from dagster_ssh import SSHResource

from .assets import huggingface, oai, ingested_study
from .resources import (
    # DBT_PROJECT_DIR,
    DATABASE_PATH,
    CollectionPublisher,
    CollectionTables,
    OAISampler,
    OaiPipeline,
)
from .sensors import staged_study_sensor, injest_and_analyze_study_job

# dbt = DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
duckdb_resource = DuckDBResource(database=DATABASE_PATH)

# dbt_assets = load_assets_from_dbt_project(DBT_PROJECT_DIR, DBT_PROJECT_DIR)
dbt_assets = []
all_assets = load_assets_from_modules([oai, huggingface, ingested_study])

stage_oai_samples_job = define_asset_job(
    "stage_oai_samples",
    [
        oai.oai_samples,
    ],
    description="Stages OAI samples",
)
run_oai_job = define_asset_job(
    "run_oai", [oai.oai_samples, oai.thickness_images], description="Run OAI on samples"
)
jobs = [stage_oai_samples_job, run_oai_job, injest_and_analyze_study_job]


resources = {
    # "dbt": dbt,
    "io_manager": DuckDBPolarsIOManager(database=DATABASE_PATH, schema="main"),
    "oai_sampler": OAISampler(oai_data_root="/mnt/cybertron/OAI", n_samples=2),
    "collection_publisher": CollectionPublisher(hf_token=EnvVar("HUGGINGFACE_TOKEN")),
    "duckdb": duckdb_resource,
    "collection_tables": CollectionTables(duckdb=duckdb_resource),
    "oai_pipeline": OaiPipeline(
        pipeline_src_dir=EnvVar("PIPELINE_SRC_DIR"),
        ssh_connection=SSHResource(
            remote_host=EnvVar("SSH_HOST"),
            username=EnvVar("SSH_USERNAME"),
            password=EnvVar("SSH_PASSWORD"),
            remote_port=22,
        ),
    ),
}

sensors = [staged_study_sensor]

defs = Definitions(
    assets=[*dbt_assets, *all_assets], resources=resources, jobs=jobs, sensors=sensors
)
