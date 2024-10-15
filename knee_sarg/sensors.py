import os
import json

from dagster import (
    sensor,
    RunRequest,
    RunConfig,
    DefaultSensorStatus,
    SensorResult,
    SensorEvaluationContext,
)

from .assets.ingested_study import study_uid_partitions_def
from .assets.oai import oai_patient_id_partitions_def
from .resources import OAISampler, FileStorage
from .jobs import ingest_and_analyze_study_job, stage_oai_sample_job


@sensor(job=ingest_and_analyze_study_job, default_status=DefaultSensorStatus.RUNNING)
def staged_study_sensor(context: SensorEvaluationContext, file_storage: FileStorage):
    """
    Sensor that triggers when a study is staged.
    """
    staged_studies_to_modified_time = (
        json.loads(context.cursor) if context.cursor else {}
    )

    staged_path = file_storage.staged_path
    run_requests = []
    partitions_to_add = []

    for collection_name in os.listdir(staged_path):
        collection_path = staged_path / collection_name
        if not os.path.isdir(collection_path):
            continue
        for uploader in os.listdir(collection_path):
            uploader_path = collection_path / uploader
            for patient_id in os.listdir(uploader_path):
                patient_path = uploader_path / patient_id
                for study_uid in os.listdir(patient_path):
                    study_uid_path = patient_path / study_uid
                    last_modified_time = max(
                        os.path.getmtime(os.path.join(study_uid_path, f))
                        for f in os.listdir(study_uid_path)
                    )
                    staged_id = f"{collection_name}-{uploader}-{patient_id}-{study_uid}"
                    existing_last_modified_time = staged_studies_to_modified_time.get(
                        staged_id
                    )
                    if last_modified_time != existing_last_modified_time:
                        staged_studies_to_modified_time[staged_id] = last_modified_time
                        run = RunRequest(
                            run_key=f"{collection_name}-{uploader}-{patient_id}-{study_uid}-{last_modified_time}",
                            partition_key=study_uid,
                            run_config=RunConfig(
                                ops={
                                    "ingested_study_files": {
                                        "config": {
                                            "collection_name": collection_name,
                                            "uploader": uploader,
                                            "study_uid": study_uid,
                                            "patient_id": patient_id,
                                        }
                                    },
                                },
                            ),
                        )
                        run_requests.append(run)
                        partitions_to_add.append(study_uid)

    context.update_cursor(json.dumps(staged_studies_to_modified_time))

    return SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=[
            study_uid_partitions_def.build_add_request(partitions_to_add)
        ],
    )


@sensor(
    job=stage_oai_sample_job,
    default_status=DefaultSensorStatus.RUNNING,
)
def patient_id_sensor(context: SensorEvaluationContext, oai_sampler: OAISampler):
    """
    Watches JSON file for Patient IDs, then creates patient_id partitions and runs oai_sample,
    only for IDs not already processed (stored in context.cursor).
    To recopy patient data that has already been processed, use the Dagster GUI to clear
    this sensors cursor.
    """
    cursor_ids = json.loads(context.cursor) if context.cursor else []

    # check for new patient IDs
    ids = oai_sampler.get_patient_ids()
    new_ids = [id for id in ids if id not in cursor_ids]

    run_requests = [
        RunRequest(
            partition_key=id,
        )
        for id in new_ids
    ]

    cursor_ids.extend(new_ids)
    context.update_cursor(json.dumps(cursor_ids))

    return SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=[
            oai_patient_id_partitions_def.build_add_request(new_ids)
        ],
    )
