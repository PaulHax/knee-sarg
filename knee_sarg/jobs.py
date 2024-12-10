from dagster import (
    define_asset_job,
)

from .ingest.ingested_study import (
    ingested_study_files,
    ingested_study_table,
    ingested_study_table_oai,
    study_uid_partitions_def,
)
from .oai.cartilage_thickness import (
    oai_samples,
    oai_sample,
    oai_patient_ids,
    cartilage_thickness,
    cartilage_thickness_oai,
    cartilage_thickness_runs,
    cartilage_thickness_runs_oai,
    oai_study,
    oai_patient_id_partitions_def,
)

stage_oai_samples_job = define_asset_job(
    "stage_oai_samples",
    [
        oai_patient_ids,
        oai_samples,
    ],
    description="Stages list of OAI patients",
)

stage_oai_sample_job = define_asset_job(
    "stage_oai_sample",
    [
        oai_sample,
    ],
    description="Stages single OAI patient",
    partitions_def=oai_patient_id_partitions_def,
)

ingest_and_analyze_study_job = define_asset_job(
    "ingest_and_analyze_study",
    [
        ingested_study_files,
        ingested_study_table,
        cartilage_thickness,
        cartilage_thickness_runs,
    ],
    description="Ingest a study into a collection and run analysis on it",
    partitions_def=study_uid_partitions_def,
    tags={"job": "gpu"},
)

cartilage_thickness_job = define_asset_job(
    "cartilage_thickness_job",
    [
        ingested_study_table,
        cartilage_thickness,
        cartilage_thickness_runs,
    ],
    description="Run cartilage thickness analysis",
    partitions_def=study_uid_partitions_def,
    tags={"job": "gpu"},
)

cartilage_thickness_oai_job = define_asset_job(
    "cartilage_thickness_oai_job",
    [
        oai_study,
        ingested_study_table_oai,
        cartilage_thickness_oai,
        cartilage_thickness_runs_oai,
    ],
    description="Sample from OAI dataset and run cartilage thickness analysis",
    partitions_def=study_uid_partitions_def,
    tags={"job": "gpu"},
)
