"""NIH Imaging Data Commons (OAI) dataset assets."""

import os
from pathlib import Path

import polars as pl
from dagster import asset, get_dagster_logger

from ..resources import OAISampler, OAI_SAMPLED_DIR, OaiPipeline

log = get_dagster_logger()


@asset()
def oai_samples(oai_sampler: OAISampler) -> pl.DataFrame:
    """
    OAI Samples. Samples are placed in data/staged/oai/dagster/.
    """
    return oai_sampler.get_samples()


pipeline_src_dir = "/home/paulhax/src/OAI_analysis_2"


def run_pipeline(ssh_resource, study_dir: Path, image_path: Path):
    with ssh_resource.get_connection() as client:
        remote_in_dir = f"{pipeline_src_dir}/in-data/"
        remote_path = f"{remote_in_dir}{os.path.basename(image_path)}"
        ssh_resource.sftp_put(remote_path, str(image_path))

        log.info("Running pipeline")
        stdin, stdout, stderr = client.exec_command(
            f"cd {pipeline_src_dir} && source ./venv/bin/activate && python ./oai_analysis/pipeline.py"
        )
        log.info(stdout.read().decode())
        if stderr_output := stderr.read().decode():
            log.error(stderr_output)

        output_dir = study_dir / "oai_output"
        output_dir.mkdir(exist_ok=True)

        remote_out_dir = f"{pipeline_src_dir}/OAI_results"
        stdin, stdout, stderr = client.exec_command(f"ls {remote_out_dir}")
        remote_files = [
            file
            for file in stdout.read().decode().splitlines()
            if file != "in_image.nrrd"
        ]
        for remote_file in remote_files:
            ssh_resource.sftp_get(
                f"{remote_out_dir}/{remote_file}", str(output_dir / remote_file)
            )


@asset(deps=[oai_samples])
def thickness_images(oai_pipeline: OaiPipeline) -> None:
    """
    Thickness Images. Generates thickness images and places them next to sampled images.
    """
    # gather images we want to run the pipeline on
    staged_images_root: str = str(OAI_SAMPLED_DIR)
    patient_dirs = [d for d in Path(staged_images_root).iterdir() if d.is_dir()]
    study_dirs = [
        subdir
        for directory in patient_dirs
        for subdir in directory.iterdir()
        if subdir.is_dir()
    ]

    def get_first_image(study_dir: Path):
        images_dir = study_dir / "nifti"
        image_paths = [
            image_path
            for series_dir in images_dir.iterdir()
            for image_path in series_dir.iterdir()
        ]
        return image_paths[0]

    study_and_image = [
        (study_dir, get_first_image(study_dir)) for study_dir in study_dirs
    ]

    for study_dir, image_path in study_and_image:
        oai_pipeline.run_pipeline(study_dir, image_path)
