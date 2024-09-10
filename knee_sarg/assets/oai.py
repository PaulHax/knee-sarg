"""NIH Imaging Data Commons (OAI) dataset assets."""

import os
from pathlib import Path

import polars as pl
from dagster import asset, get_dagster_logger
from dagster_ssh import SSHResource

from ..resources import OAISampler, OAI_SAMPLED_DIR

log = get_dagster_logger()

@asset()
def oai_samples(oai_sampler: OAISampler) -> pl.DataFrame:
    """
    OAI Samples. Samples are placed in data/staged/oai/dagster/.
    """
    return oai_sampler.get_samples()

pipeline_src_dir = '/home/paulhax/src/OAI_analysis_2'

def run_pipeline(ssh_resource, client, study_dir: Path):
    images_dir  = study_dir / "nifti"
    image_paths = [image_path for series_dir in images_dir.iterdir() for image_path in series_dir.iterdir()]
    first_image_path = str(image_paths[0])
    file_name = os.path.basename(first_image_path)
    remote_in_dir = f"{pipeline_src_dir}/in-data/"
    remote_path = f"{remote_in_dir}{file_name}"
    ssh_resource.sftp_put(remote_path, first_image_path)

    log.info("Running pipeline")
    stdin, stdout, stderr = client.exec_command(f"cd {pipeline_src_dir} && source ./venv/bin/activate && python ./oai_analysis/pipeline.py")
    log.info(stdout.read().decode())
    stderr_output = stderr.read().decode()
    if stderr_output:
        log.error(stderr_output)
    
    output_dir = study_dir / "oai_output"
    output_dir.mkdir(exist_ok=True)

    remote_out_dir = f"{pipeline_src_dir}/OAI_results"
    stdin, stdout, stderr = client.exec_command(f"ls {remote_out_dir}")
    remote_files = stdout.read().decode().splitlines()
    remote_files = [file for file in remote_files if file != "in_image.nrrd"]
    for remote_file in remote_files:
        remote_path = f"{remote_out_dir}/{remote_file}"
        local_path = output_dir / remote_file
        ssh_resource.sftp_get(remote_path, str(local_path))
    

def run_on_samples(ssh_resource, client):
    staged_images_root: str = str(OAI_SAMPLED_DIR)
    patient_dirs = [d for d in Path(staged_images_root).iterdir() if d.is_dir()]
    study_dirs = [subdir for directory in patient_dirs for subdir in directory.iterdir() if subdir.is_dir()]
    for study_dir in study_dirs:
        run_pipeline(ssh_resource, client, study_dir)

@asset(deps=[oai_samples])
def thickness_images(ssh_compute: SSHResource) -> None:
    """
    Thickness Images. Generates thickness images and places them in data/results/oai/dagster/.
    """
    client = ssh_compute.get_connection()
    try:
        run_on_samples(ssh_compute, client)
    finally:
        client.close()
