import os
import tempfile
from typing import Dict, Optional, List, Any
from pathlib import Path
import shutil
import json
import subprocess
from abc import ABC, abstractmethod

import yaml
import pandas as pd
from dagster import (
    InitResourceContext,
    ConfigurableResource,
    ResourceDependency,
    get_dagster_logger,
)
from dagster_duckdb import DuckDBResource
from dagster_ssh import SSHResource
from pydantic import PrivateAttr
from huggingface_hub import HfApi
import pyarrow.csv as csv
import itk

from scripts.cartilage_thickness_collection import FilePaths

log = get_dagster_logger()

# Dagster working state
DBT_PROJECT_DIR = str(Path(__file__).parent.resolve() / ".." / "dbt")
DATA_DIR = Path(__file__).parent.resolve() / ".." / "data"
DATABASE_PATH = os.getenv("DATABASE_PATH", str(DATA_DIR / "database.duckdb"))
SLURM_LOGS_DIR = str(DATA_DIR / "slurm-logs")

OAI_COLLECTION_NAME = "oai"

collection_table_names = {"patients", "studies", "series"}

StudyInfo = Dict[str, Any]


class FileStorage(ConfigurableResource):
    root_dir: str = str(DATA_DIR)
    staged_dir: str = ""
    ingested_dir: str = ""
    collections_dir: str = ""

    def setup_for_execution(self, _: InitResourceContext) -> None:
        self._file_paths = FilePaths(
            root_dir=self.root_dir,
            staged_dir=self.staged_dir,
            ingested_dir=self.ingested_dir,
            collections_dir=self.collections_dir,
        )

    @property
    def staged_path(self) -> Path:
        return self._file_paths.staged_path

    @property
    def ingested_path(self) -> Path:
        return self._file_paths.ingested_path

    @property
    def collections_path(self) -> Path:
        return self._file_paths.collections_path

    def ensure_collection_dir(self, collection: str):
        return self._file_paths.ensure_collection_dir(collection)

    def get_study_collection_dir(
        self,
        collection: str,
        study_info: StudyInfo,
    ) -> Path:
        return self._file_paths.get_study_collection_dir(collection, study_info)

    def get_output_dir(
        self,
        collection: str,
        study_info: StudyInfo,
        analysis_name: str,
        code_version: str = "undefined",
    ) -> Path:
        return self._file_paths.get_output_dir(
            collection, study_info, analysis_name, code_version
        )

    def make_output_dir(
        self,
        collection: str,
        dir_info: StudyInfo,
        analysis_name: str,
        code_version: str = "None",
    ) -> Path:
        return self._file_paths.make_output_dir(
            collection, dir_info, analysis_name, code_version
        )


class CollectionTables(ConfigurableResource):
    duckdb: ResourceDependency[DuckDBResource]
    file_storage: ResourceDependency[FileStorage]
    collection_names: List[str] = [OAI_COLLECTION_NAME]

    def setup_for_execution(self, context: InitResourceContext) -> None:
        os.makedirs(self.file_storage.collections_path, exist_ok=True)
        self._db = self.duckdb

        with self._db.get_connection() as conn:
            for collection_name in self.collection_names:
                collection_path = self.file_storage.collections_path / collection_name
                os.makedirs(collection_path, exist_ok=True)

                for table in collection_table_names:
                    table_parquet = collection_path / f"{table}.parquet"
                    table_name = f"{collection_name}_{table}"
                    if table_parquet.exists():
                        conn.execute(
                            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM parquet_scan('{table_parquet}')"
                        )
                    else:
                        if table == "patients":
                            conn.execute(
                                f"CREATE TABLE IF NOT EXISTS {table_name} (patient_id VARCHAR);"
                            )
                        elif table == "studies":
                            conn.execute(
                                f"CREATE TABLE IF NOT EXISTS {table_name} (patient_id VARCHAR, study_uid VARCHAR, study_date DATE, study_description VARCHAR);"
                            )
                        elif table == "series":
                            conn.execute(
                                f"CREATE TABLE IF NOT EXISTS {table_name} (patient_id VARCHAR, study_uid VARCHAR, series_uid VARCHAR, series_number BIGINT, modality VARCHAR, body_part_examined VARCHAR, series_description VARCHAR);"
                            )

    def teardown_after_execution(self, _: InitResourceContext) -> None:
        with self._db.get_connection() as conn:
            conn.execute("VACUUM")

    def write_collection_parquets(self):
        with self._db.get_connection() as conn:
            for collection_name in self.collection_names:
                collection_path = self.file_storage.collections_path / collection_name
                for table in collection_table_names:
                    table_name = f"{collection_name}_{table}"
                    table_parquet = collection_path / f"{table}.parquet"
                    conn.execute(
                        f"COPY {table_name} TO '{table_parquet}' (FORMAT 'parquet')"
                    )

    def insert_into_collection(
        self, collection_name: str, table_name: str, df: pd.DataFrame
    ):
        if df.empty:
            return
        if collection_name not in self.collection_names:
            raise ValueError(f"Collection {collection_name} not found")
        if table_name not in collection_table_names:
            raise ValueError(f"Table {table_name} not found")

        with self._db.get_connection() as conn:
            conn.execute(f"INSERT INTO {collection_name}_{table_name} SELECT * FROM df")


class CartilageThicknessTable(ConfigurableResource):
    duckdb: DuckDBResource
    file_storage: FileStorage
    name: str = "cartilage_thickness_runs"
    collection_name: str = OAI_COLLECTION_NAME

    def setup_for_execution(self, _: InitResourceContext) -> None:
        collection_path = self.file_storage.collections_path / self.collection_name
        os.makedirs(collection_path, exist_ok=True)
        self._table_path = collection_path / f"{self.name}.parquet"
        self._table_name = f"{self.collection_name}_{self.name}"

        self._db = self.duckdb

        with self._db.get_connection() as conn:
            if self._table_path.exists():
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {self._table_name} AS SELECT * FROM parquet_scan('{self._table_path}')"
                )
            else:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {self._table_name} (patient_id VARCHAR, study_uid VARCHAR, series_uid VARCHAR, computed_files_dir VARCHAR, code_version VARCHAR);"
                )

    def teardown_after_execution(self, _: InitResourceContext) -> None:
        with self._db.get_connection() as conn:
            conn.execute("VACUUM")

    def insert_run(self, df: pd.DataFrame):
        if df.empty:
            return
        with self._db.get_connection() as conn:
            conn.execute(f"INSERT INTO {self._table_name} SELECT * FROM df")

    def write_incremental_parquet(self, df: pd.DataFrame):
        if df.empty:
            return

        if self._table_path.exists():
            existing_df = pd.read_parquet(self._table_path)
            df = pd.concat([existing_df, df])

        df.to_parquet(self._table_path, index=False)


class OAISampler(ConfigurableResource):
    # directory with OAI data as provided by the OAI
    oai_data_root: str
    file_storage: ResourceDependency[FileStorage]
    oai_sampler_dir: str = str(DATA_DIR / "oai-sampler")
    patient_ids_file: str = str(DATA_DIR / "oai-sampler" / "patient_ids.json")

    def get_time_point_folders(self) -> Dict[int, str]:
        # all months = [0, 12, 18, 24, 30, 36, 48, 72, 96]
        # Most did not have time point 30
        time_points = [0, 12, 18, 24, 36, 48, 72, 96]
        time_point_folders = {
            **{0: "OAIBaselineImages"},
            **{m: f"OAI{m}MonthImages" for m in time_points[1:]},
        }
        return time_point_folders

    def setup_for_execution(self, _: InitResourceContext) -> None:
        time_point_folders = self.get_time_point_folders()

        patients_dfs = []

        for time_point_folder in time_point_folders.values():
            patients_file_path = (
                Path(self.oai_data_root) / time_point_folder / "enrollee01.txt"
            )
            patients_table = csv.read_csv(
                patients_file_path,
                parse_options=csv.ParseOptions(delimiter="\t"),
                read_options=csv.ReadOptions(skip_rows_after_names=1),
            )
            patients_df_tp = patients_table.to_pandas()
            patients_dfs.append(patients_df_tp)

        # stack rows from all enrollee01.txt files
        self._patients_df = pd.concat(patients_dfs, ignore_index=True)

        self._study_to_vol_path = pd.read_csv(
            DATA_DIR / "oai-sampler" / "study_uid_to_vol_path.csv", dtype=str
        )

    def get_patient_info(self, patient_id: str) -> pd.DataFrame:
        return self._patients_df.loc[
            self._patients_df["src_subject_id"] == int(patient_id)
        ].iloc[0]

    def get_study_info(self, study_uid: str) -> pd.DataFrame:
        return self._study_to_vol_path.loc[
            self._study_to_vol_path["study_uid"] == study_uid
        ].iloc[0]

    def get_patient_ids(self, file_name: Optional[str] = None) -> List[str]:
        if file_name:
            file_path = Path(self.oai_sampler_dir) / file_name
        else:
            file_path = self.patient_ids_file

        if not os.path.exists(file_path):
            return []
        with open(file_path, "r") as fp:
            target_patients = json.load(fp)
        return target_patients

    # patient ids: "9000798", "9007827", "9016304"
    def get_samples(self, patient_id: str) -> pd.DataFrame:
        dess_file = DATA_DIR / "oai-sampler" / "SEG_3D_DESS_all.csv"
        dess_df = pd.read_csv(dess_file)

        patient_info = self.get_patient_info(patient_id)

        from_patient_info = ["gender"]
        result = pd.DataFrame(
            columns=[*from_patient_info, "patient_id", "study_uid", "month"]
        ).astype(
            {
                "patient_id": "string",
                "study_uid": "string",
                "gender": "string",
                "month": "int32",
            }
        )

        for month, time_point_folder in self.get_time_point_folders().items():
            folder = Path(self.oai_data_root) / Path(time_point_folder) / "results"
            # filter out zip files
            for study in [study for study in folder.iterdir() if study.is_dir()]:
                for patient_dir in study.iterdir():
                    if patient_dir.match(patient_id):
                        acquisition_id = str(patient_dir.relative_to(folder)).replace(
                            "\\",  # when Windows uses backslash, convert to Linux style
                            "/",
                        )
                        acquisition_dess = dess_df["Folder"].str.contains(
                            acquisition_id
                        )
                        acquisition_df = dess_df.loc[acquisition_dess, :]
                        log.info(f"Fetching images for patient {patient_id}")

                        for _, description in acquisition_df.iterrows():
                            vol_folder = folder / description["Folder"]
                            if not vol_folder.exists():
                                continue

                            frame_0 = itk.imread(vol_folder / os.listdir(vol_folder)[0])
                            meta = dict(frame_0)
                            image = itk.imread(str(vol_folder))

                            study_uid = meta["0020|000d"]
                            series_uid = meta["0020|000e"]

                            output_dir = (
                                self.file_storage.staged_path
                                / "oai"
                                / "dagster"
                                / str(patient_id)
                                / str(study_uid)
                            )
                            os.makedirs(output_dir, exist_ok=True)

                            study_date = meta.get("0008|0020", "00000000")
                            study_date = (
                                f"{study_date[4:6]}-{study_date[6:8]}-{study_date[:4]}"
                            )
                            study_description = meta.get("0008|1030", "").strip()
                            series_number = meta.get("0020|0011", "0")
                            series_number = int(series_number)
                            modality = meta.get("0008|0060", "").strip()
                            body_part_examined = meta.get("0018|0015", "")
                            series_description = meta.get("0008|103e", "").strip()
                            log.info(
                                f"{study_date} {study_description} {series_number} {modality} {body_part_examined} {series_description}"
                            )

                            study = {
                                "patient_id": patient_id,
                                "study_uid": study_uid,
                                "study_date": study_date,
                                "study_description": study_description,
                            }

                            series = {
                                "patient_id": patient_id,
                                "study_uid": study_uid,
                                "series_uid": series_uid,
                                "series_number": series_number,
                                "modality": modality,
                                "body_part_examined": body_part_examined,
                                "series_description": series_description,
                            }

                            staged_study_path = (
                                self.file_storage.staged_path
                                / OAI_COLLECTION_NAME
                                / "dagster"
                                / patient_id
                                / study_uid
                            )

                            patient_info["month"] = month
                            patient_info.to_json(
                                path_or_buf=staged_study_path / "oai.json",
                            )

                            patient_json = patient_info[from_patient_info]
                            patient_json["patient_id"] = patient_id
                            result.loc[len(result)] = patient_json

                            patient_json.to_json(
                                path_or_buf=staged_study_path / "patient.json",
                            )

                            with open(staged_study_path / "study.json", "w") as f:
                                json.dump(study, f)

                            with open(staged_study_path / "series.json", "w") as f:
                                json.dump(series, f)

                            nifti_path = staged_study_path / "nifti" / series_uid
                            os.makedirs(nifti_path, exist_ok=True)
                            itk.imwrite(image, nifti_path / "image.nii.gz")

        return result


class OaiPipeline(ConfigurableResource, ABC):
    @abstractmethod
    def run_pipeline(
        self,
        image_path: str,
        output_dir: str,
        laterality: str,
        run_id: str,
        override_src_dir: Optional[str] = None,
    ):
        pass


class OaiPipelineSSH(OaiPipeline):
    ssh_connection: ResourceDependency[SSHResource]
    pipeline_src_dir: str
    env_setup_command: str = ""

    def run_pipeline(
        self,
        image_path: str,
        output_dir: str,
        laterality: str,
        run_id: str,
        override_src_dir: Optional[str] = None,
    ):
        src_dir = override_src_dir or self.pipeline_src_dir
        with self.ssh_connection.get_connection() as client:
            temp_dir = f"{src_dir}/temp/{run_id}"
            remote_in_dir = f"{temp_dir}/in-data/"
            stdin, stdout, stderr = client.exec_command(f"mkdir -p {remote_in_dir}")
            remote_image_path = f"{remote_in_dir}/{os.path.basename(image_path)}"
            self.ssh_connection.sftp_put(remote_image_path, image_path)

            optional_env_setup = (
                f"{self.env_setup_command} && " if self.env_setup_command else ""
            )
            remote_out_dir = f"{temp_dir}/oai_results"
            run_call = f"python ./oai_analysis/pipeline_cli.py {remote_image_path} {remote_out_dir} {laterality}"
            log.info(f"Running pipeline: {run_call}")
            stdin, stdout, stderr = client.exec_command(
                f"cd {src_dir} && source ./venv/bin/activate && {optional_env_setup} {run_call}"
            )
            log.info(stdout.read().decode())
            if stderr_output := stderr.read().decode():
                log.error(stderr_output)

            stdin, stdout, stderr = client.exec_command(f"ls {remote_out_dir}")
            remote_files = [
                file
                for file in stdout.read().decode().splitlines()
                if file
                != "in_image.nrrd"  # skip original image as may be unathorized to distribute in collection
            ]

            for remote_file in remote_files:
                self.ssh_connection.sftp_get(
                    f"{remote_out_dir}/{remote_file}",
                    str(Path(output_dir) / remote_file),
                )

            stdin, stdout, stderr = client.exec_command(f"rm -rf {temp_dir}")
            log.info(stdout.read().decode())
            if stderr_output := stderr.read().decode():
                log.error(stderr_output)


class OaiPipelineSubprocess(OaiPipeline):
    pipeline_src_dir: str
    env_setup_command: str = ""

    def run_pipeline(
        self,
        image_path: str,
        output_dir: str,
        laterality: str,
        _: str,  # run id
        override_src_dir: Optional[str] = None,
    ):
        src_dir = override_src_dir or self.pipeline_src_dir
        optional_env_setup = (
            f"{self.env_setup_command} && " if self.env_setup_command else ""
        )
        run_call = f'python ./oai_analysis/pipeline_cli.py "{image_path}" "{output_dir}" {laterality}'
        command = f"{optional_env_setup}{run_call}"
        log.info(f"Running pipeline: {run_call}")
        log.info(f"With env setup: {command}")

        result = subprocess.run(
            command, cwd=src_dir, shell=True, capture_output=True, text=True
        )
        log.info(result.stdout)
        if result.stderr:
            log.error(result.stderr)


class OaiPipelineSlurm(OaiPipeline):
    pipeline_src_dir: str
    sbatch_args: str = ""

    def run_pipeline(
        self,
        image_path: str,
        output_dir: str,
        laterality: str,
        run_id: str,  # run id
        override_src_dir: Optional[str] = None,
    ):
        src_dir = override_src_dir or self.pipeline_src_dir
        command = f'sbatch --parsable --wait --output="{SLURM_LOGS_DIR}/output_%j.log" --error="{SLURM_LOGS_DIR}/error_%j.log" --job-name=oai-analysis-{run_id} {self.sbatch_args} ./scripts/cartilage-thickness-slurm.sh "{image_path}" "{output_dir}" {laterality} "{src_dir}"'
        log.info(f"Pipeline command:\n{command}")

        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        job_id = result.stdout.strip()

        output_file = f"{SLURM_LOGS_DIR}/output_{job_id}.log"
        error_file = f"{SLURM_LOGS_DIR}/error_{job_id}.log"

        if os.path.exists(output_file):
            with open(output_file, "r") as f:
                output_content = f.read()
                if output_content:
                    log.info(output_content)
        else:
            log.error(f"Output file {output_file} not found.")

        if os.path.exists(error_file):
            with open(error_file, "r") as f:
                error_content = f.read()
                if error_content:
                    log.error(error_content)
        else:
            log.error(f"Error file {error_file} not found.")

        if result.returncode != 0:
            raise RuntimeError(
                f"Slurm sbatch failed with return code {result.returncode}"
            )


class CollectionPublisher(ConfigurableResource):
    file_storage: ResourceDependency[FileStorage]
    hf_token: str
    tmp_dir: str = tempfile.gettempdir()

    _api: HfApi = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._api = HfApi(token=self.hf_token)

    def publish(
        self,
        collection_name: str,
        readme: Optional[str] = None,
        generate_data_package: bool = False,
    ):
        with tempfile.TemporaryDirectory(dir=self.tmp_dir) as temp_dir:
            collection_path = self.file_storage.collections_path / collection_name
            log.info(f"Copying collection {collection_name} to {temp_dir}")
            shutil.copytree(collection_path, temp_dir, dirs_exist_ok=True)

            if readme:
                readme_path = os.path.join(temp_dir, "README.md")
                with open(readme_path, "w") as readme_file:
                    readme_file.write(readme)

            if generate_data_package:
                data_package = {
                    "name": collection_name,
                    "resources": [
                        {"path": "patients.parquet", "format": "parquet"},
                        {"path": "studies.parquet", "format": "parquet"},
                        {"path": "series.parquet", "format": "parquet"},
                    ],
                }
                data_package_path = os.path.join(temp_dir, "datapackage.yaml")
                with open(data_package_path, "w") as dp_file:
                    yaml.dump(data_package, dp_file)

            log.info(f"Uploading collection {collection_name} to Hugging Face")
            # Note: the repository has to be already created
            self._api.upload_folder(
                folder_path=temp_dir,
                repo_id=f"radiogenomics/knee_sarg_{collection_name}",
                repo_type="dataset",
                commit_message=f"Update {collection_name} collection",
            )
