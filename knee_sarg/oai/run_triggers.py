from pydantic import Field
from pathlib import Path
from dagster import (
    asset,
    Config,
    AssetKey,
    EventLogEntry,
    SensorResult,
    RunRequest,
    SensorEvaluationContext,
    asset_sensor,
    DefaultSensorStatus,
)
from dagster_duckdb import DuckDBResource
import os
import json
import polars as pl
import pandas as pd

from ..resources import (
    DATA_DIR,
)
from ..ingest.ingested_study import study_uid_partitions_def
from ..jobs import (
    cartilage_thickness_oai_job,
)


class StudyUidConfig(Config):
    study_uid_file: str = Field(
        default_factory=lambda: "study_uids_to_run.json",
        description="JSON file with array of study UIDs",
    )


@asset()
def oai_study_uids_to_run(
    config: StudyUidConfig,
) -> pl.DataFrame:
    """
    Reads study UIDs from a JSON file. Default file is DATA/oai-sampler/study_uids_to_run.json.
    """
    file_path = Path(DATA_DIR) / "oai-sampler" / config.study_uid_file
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r") as fp:
        ids = json.load(fp)

    return pl.from_pandas(
        pd.DataFrame(
            [
                {
                    "study_uid": id,
                }
                for id in ids
            ]
        )
    )


@asset_sensor(
    default_status=DefaultSensorStatus.RUNNING,
    asset_key=AssetKey("oai_study_uids_to_run"),
    job=cartilage_thickness_oai_job,
)
def oai_study_uids_to_run_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry, duckdb: DuckDBResource
):
    with duckdb.get_connection() as conn:
        result = conn.execute("SELECT * FROM main.oai_study_uids_to_run").fetchall()
    uids = [row[0] for row in result]

    partitions_to_add = [
        uid
        for uid in uids
        if not context.instance.has_dynamic_partition(
            study_uid_partitions_def.name, uid
        )
    ]
    run_requests = [
        RunRequest(
            partition_key=id,
        )
        for id in uids
    ]

    return SensorResult(
        dynamic_partitions_requests=[
            study_uid_partitions_def.build_add_request(partitions_to_add)
        ],
        run_requests=run_requests,
    )
