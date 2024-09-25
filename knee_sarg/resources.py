import os
import tempfile
from typing import Dict, Optional, List, Any
from pathlib import Path
import shutil
import json

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

log = get_dagster_logger()

DBT_PROJECT_DIR = str(Path(__file__).parent.resolve() / ".." / "dbt")
DATA_DIR = Path(__file__).parent.resolve() / ".." / "data"
STAGED_DIR = DATA_DIR / "staged"
OAI_SAMPLED_DIR = STAGED_DIR / "oai" / "dagster"
INGESTED_DIR = DATA_DIR / "ingested"
COLLECTIONS_DIR = DATA_DIR / "collections"
DATABASE_PATH = os.getenv("DATABASE_PATH", str(DATA_DIR / "database.duckdb"))

OAI_COLLECTION_NAME = "oai"

collection_table_names = {"patients", "studies", "series"}

StudyInfo = Dict[str, Any]


class CollectionTables(ConfigurableResource):
    duckdb: DuckDBResource
    collection_names: List[str] = [OAI_COLLECTION_NAME]

    def setup_for_execution(self, context: InitResourceContext) -> None:
        os.makedirs(COLLECTIONS_DIR, exist_ok=True)
        self._db = self.duckdb

        with self._db.get_connection() as conn:
            for collection_name in self.collection_names:
                collection_path = COLLECTIONS_DIR / collection_name
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
                                f"CREATE TABLE IF NOT EXISTS {table_name} (patient_id VARCHAR, gender VARCHAR, ethnicity VARCHAR, race VARCHAR);"
                            )
                        elif table == "studies":
                            conn.execute(
                                f"CREATE TABLE IF NOT EXISTS {table_name} (patient_id VARCHAR, study_instance_uid VARCHAR, study_date DATE, study_description VARCHAR);"
                            )
                        elif table == "series":
                            conn.execute(
                                f"CREATE TABLE IF NOT EXISTS {table_name} (patient_id VARCHAR, study_instance_uid VARCHAR, series_instance_uid VARCHAR, series_number BIGINT, modality VARCHAR, body_part_examined VARCHAR, series_description VARCHAR);"
                            )

    def teardown_after_execution(self, _: InitResourceContext) -> None:
        with self._db.get_connection() as conn:
            conn.execute("VACUUM")

    def write_collection_parquets(self):
        with self._db.get_connection() as conn:
            for collection_name in self.collection_names:
                collection_path = COLLECTIONS_DIR / collection_name
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
    name: str = "cartilage_thickness_runs"
    collection_name: str = OAI_COLLECTION_NAME

    def setup_for_execution(self, _: InitResourceContext) -> None:
        collection_path = COLLECTIONS_DIR / self.collection_name
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
                    f"CREATE TABLE IF NOT EXISTS {self._table_name} (patient_id VARCHAR, study_uid VARCHAR, series_id VARCHAR, computed_files_dir VARCHAR);"
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

    # def write_parquets(self):
    #     with self._db.get_connection() as conn:
    #         collection_name = self.collection_name
    #         collection_path = COLLECTIONS_DIR / collection_name
    #         table_parquet = collection_path / f"{self.name}.parquet"
    #         conn.execute(
    #             f"COPY {self.table_name} TO '{table_parquet}' (FORMAT 'parquet')"
    #         )


class OAISampler(ConfigurableResource):
    # directory with OAI data as provided by the OAI
    oai_data_root: str

    def get_time_point_folders(self) -> Dict[int, str]:
        # months
        # time_points = [0, 12, 18, 24, 30, 36, 48, 72, 96]
        # Most did not have time point 30
        time_points = [0, 12, 18, 24, 36, 48, 72, 96]
        time_point_folders = {
            **{0: "OAIBaselineImages"},
            **{m: f"OAI{m}MonthImages" for m in time_points[1:]},
        }
        return time_point_folders

    def get_patient_ids(self, target_patient_file: str) -> List[str]:
        with open(DATA_DIR / "oai-sampler" / target_patient_file, "r") as fp:
            target_patients = json.load(fp)
        return target_patients

    # patient ids: "9000798", "9007827", "9016304"
    def get_samples(self, patient_id: str) -> pd.DataFrame:
        dess_file = DATA_DIR / "oai-sampler" / "SEG_3D_DESS_all.csv"
        dess_df = pd.read_csv(dess_file)

        time_point_folders = self.get_time_point_folders()

        first_folder = list(time_point_folders.values())[0]
        patients_file_path = Path(self.oai_data_root) / first_folder / "enrollee01.txt"
        patients_table = csv.read_csv(
            patients_file_path,
            parse_options=csv.ParseOptions(delimiter="\t"),
            read_options=csv.ReadOptions(skip_rows=2, autogenerate_column_names=True),
        )
        patients_df = patients_table.to_pandas()

        columns_to_include = ["patient_id", "gender", "ethnicity", "race"]
        column_ids_to_include = [4, 7, 9, 18]

        patients_df = patients_df.iloc[:, column_ids_to_include]
        patients_df.columns = columns_to_include

        for time_point_folder in list(time_point_folders.values())[1:]:
            patients_file_path = (
                Path(self.oai_data_root) / time_point_folder / "enrollee01.txt"
            )
            patients_table = csv.read_csv(
                patients_file_path,
                parse_options=csv.ParseOptions(delimiter="\t"),
                read_options=csv.ReadOptions(
                    skip_rows=2, autogenerate_column_names=True
                ),
            )
            patients_df_tp = patients_table.to_pandas()

            patients_df_tp = patients_df_tp.iloc[:, column_ids_to_include]
            patients_df_tp.columns = columns_to_include

            patients_df = pd.concat([patients_df, patients_df_tp])

        result = pd.DataFrame(columns=[*columns_to_include, "month", "series_id"])
        result = result.astype(
            {
                "patient_id": "string",
                "series_id": "string",
                "gender": "string",
                "ethnicity": "string",
                "race": "string",
                "month": "int32",
            }
        )

        for time_point_folder in time_point_folders.values():
            folder = Path(self.oai_data_root) / Path(time_point_folder) / "results"
            # filter out zip files
            for study in [study for study in folder.iterdir() if study.is_dir()]:
                for patient_dir in study.iterdir():
                    if patient_dir.match(patient_id):
                        acquisition_id = patient_dir.relative_to(folder)
                        acquisition_dess = dess_df["Folder"].str.contains(
                            str(acquisition_id)
                        )
                        acquisition_df = dess_df.loc[acquisition_dess, :]
                        log.info(f"Fetching images for patient {patient_id}")

                        studies_table = pd.DataFrame(
                            columns=[
                                "patient_id",
                                "study_instance_uid",
                                "study_date",
                                "study_description",
                            ]
                        )
                        studies_table.astype(
                            {
                                "patient_id": "string",
                                "study_instance_uid": "string",
                                "study_date": "string",
                                "study_description": "string",
                            }
                        )

                        series_table = pd.DataFrame(
                            columns=[
                                "patient_id",
                                "study_instance_uid",
                                "series_instance_uid",
                                "series_number",
                                "modality",
                                "body_part_examined",
                                "series_description",
                            ]
                        )
                        series_table.astype(
                            {
                                "patient_id": "string",
                                "study_instance_uid": "string",
                                "series_instance_uid": "string",
                                "series_number": "int32",
                                "modality": "string",
                                "body_part_examined": "string",
                                "series_description": "string",
                            }
                        )

                        for _, descr in acquisition_df.iterrows():
                            # is_left = descr['SeriesDescription'].find('LEFT') > -1
                            vol_folder = folder / descr["Folder"]
                            if not vol_folder.exists():
                                continue
                            frame_0 = itk.imread(vol_folder / os.listdir(vol_folder)[0])
                            meta = dict(frame_0)
                            image = itk.imread(str(vol_folder))

                            study_instance_uid = meta["0020|000d"]
                            series_instance_uid = meta["0020|000e"]
                            log.info(f"Study Instance UID: {study_instance_uid}")
                            log.info(f"Series Instance UID: {series_instance_uid}")

                            output_dir = (
                                STAGED_DIR
                                / "oai"
                                / "dagster"
                                / str(patient_id)
                                / str(study_instance_uid)
                            )
                            os.makedirs(output_dir, exist_ok=True)

                            study_date = meta.get("0008|0020", "00000000")
                            study_date = (
                                f"{study_date[4:6]}-{study_date[6:8]}-{study_date[:4]}"
                            )
                            study_description = meta.get("0008|1030", "")
                            series_number = meta.get("0020|0011", "0")
                            series_number = int(series_number)
                            modality = meta.get("0008|0060", "").strip()
                            body_part_examined = meta.get("0018|0015", "")
                            series_description = meta.get("0008|103e", "")
                            log.info(
                                f"{study_date} {study_description} {series_number} {modality} {body_part_examined} {series_description}"
                            )

                            studies_table.loc[len(studies_table)] = {
                                "patient_id": patient_id,
                                "study_instance_uid": study_instance_uid,
                                "study_date": study_date,
                                "study_description": study_description,
                            }
                            series_table.loc[len(series_table)] = {
                                "patient_id": patient_id,
                                "study_instance_uid": study_instance_uid,
                                "series_instance_uid": series_instance_uid,
                                "series_number": series_number,
                                "modality": modality,
                                "body_part_examined": body_part_examined,
                                "series_description": series_description,
                            }

                            staged_study_path = (
                                STAGED_DIR
                                / OAI_COLLECTION_NAME
                                / "dagster"
                                / patient_id
                                / study_instance_uid
                            )

                            # print(
                            #     "patients_df",
                            #     patients_df.loc[
                            #         patients_df["patient_id"] == int(patient)
                            #     ],
                            # )
                            # print(patients_df["patient_id"].head())
                            row = patients_df.loc[
                                patients_df["patient_id"] == int(patient_id)
                            ].iloc[0]
                            row["patient_id"] = patient_id

                            result.loc[len(result)] = row

                            with open(staged_study_path / "patient.json", "w") as fp:
                                fp.write(pd.DataFrame({0: row}).to_json())

                            with open(staged_study_path / "study.json", "w") as fp:
                                fp.write(studies_table.to_json())

                            with open(staged_study_path / "series.json", "w") as fp:
                                fp.write(series_table.to_json())

                            nifti_path = (
                                staged_study_path / "nifti" / series_instance_uid
                            )
                            os.makedirs(nifti_path, exist_ok=True)
                            itk.imwrite(image, nifti_path / "image.nii.gz")

        return result


def make_output_dir(collection: str, dir_info: StudyInfo, analysis_name: str):
    patient, study = (
        dir_info["patient"],
        dir_info["study"],
    )
    study_dir = COLLECTIONS_DIR / collection / patient / study
    output_dir = study_dir / analysis_name

    if output_dir.exists():
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    return output_dir


point_to_tbb_in_user_space = "export LD_LIBRARY_PATH=$HOME/oneapi-tbb-2021.5.0/lib/intel64/gcc4.8:$LD_LIBRARY_PATH"


class OaiPipeline(ConfigurableResource):
    ssh_connection: ResourceDependency[SSHResource]
    pipeline_src_dir: str
    env_setup_command: str = ""

    def run_pipeline(
        self,
        image_path: str,
        output_dir: str,
        run_id: str,
    ):
        with self.ssh_connection.get_connection() as client:
            temp_dir = f"{self.pipeline_src_dir}/temp/{run_id}"
            remote_in_dir = f"{temp_dir}/in-data/"
            stdin, stdout, stderr = client.exec_command(f"mkdir -p {remote_in_dir}")
            remote_image_path = f"{remote_in_dir}/{os.path.basename(image_path)}"
            self.ssh_connection.sftp_put(remote_image_path, image_path)

            optional_env_setup = (
                f"{self.env_setup_command} && " if self.env_setup_command else ""
            )
            remote_out_dir = f"{temp_dir}/oai_results"
            run_call = f"python ./oai_analysis/pipeline_cli.py {remote_image_path} {remote_out_dir}"
            log.info(f"Running pipeline: {run_call}")
            stdin, stdout, stderr = client.exec_command(
                f"cd {self.pipeline_src_dir} && source ./venv/bin/activate && {optional_env_setup} {run_call}"
            )
            log.info(stdout.read().decode())
            if stderr_output := stderr.read().decode():
                log.error(stderr_output)

            stdin, stdout, stderr = client.exec_command(f"ls {remote_out_dir}")
            remote_files = [
                file
                for file in stdout.read().decode().splitlines()
                if file != "in_image.nrrd"
            ]

            os.makedirs(output_dir, exist_ok=True)
            for remote_file in remote_files:
                self.ssh_connection.sftp_get(
                    f"{remote_out_dir}/{remote_file}",
                    str(Path(output_dir) / remote_file),
                )

            stdin, stdout, stderr = client.exec_command(f"rm -rf {temp_dir}")
            log.info(stdout.read().decode())
            if stderr_output := stderr.read().decode():
                log.error(stderr_output)


class CollectionPublisher(ConfigurableResource):
    hf_token: str
    tmp_dir: str = tempfile.gettempdir()

    _api: HfApi = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._api = HfApi(token=self.hf_token)

    def publish(
        self,
        collection_name: str,
        readme: Optional[str] = None,
        generate_datapackage: bool = False,
    ):
        with tempfile.TemporaryDirectory(dir=self.tmp_dir) as temp_dir:
            collection_path = COLLECTIONS_DIR / collection_name
            log.info(f"Copying collection {collection_name} to {temp_dir}")
            shutil.copytree(collection_path, temp_dir, dirs_exist_ok=True)

            if readme:
                readme_path = os.path.join(temp_dir, "README.md")
                with open(readme_path, "w") as readme_file:
                    readme_file.write(readme)

            if generate_datapackage:
                datapackage = {
                    "name": collection_name,
                    "resources": [
                        {"path": "patients.parquet", "format": "parquet"},
                        {"path": "studies.parquet", "format": "parquet"},
                        {"path": "series.parquet", "format": "parquet"},
                    ],
                }
                datapackage_path = os.path.join(temp_dir, "datapackage.yaml")
                with open(datapackage_path, "w") as dp_file:
                    yaml.dump(datapackage, dp_file)

            log.info(f"Uploading collection {collection_name} to Hugging Face")
            # Note: the repository has to be already created
            self._api.upload_folder(
                folder_path=temp_dir,
                repo_id=f"radiogenomics/knee_sarg_{collection_name}",
                repo_type="dataset",
                commit_message=f"Update {collection_name} collection",
            )
