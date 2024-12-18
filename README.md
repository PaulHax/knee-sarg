# Knee-SARG

Knee-SARG is a fully open-source and local-first platform image analysis tool.

## Features

-   **Environment Setup with Pixi**: Utilizes Pixi for managing reproducible software environments and CLI tasks.
-   **OAI Cartilage Thickness Pipeline**: Image processing pipeline for Knee MRIs with the `OAI_analysis_2` codebase.
-   **Dagster Orchestration**:
    -   Manages data assets and pipeline execution using partitions corresponding to study UIDs and patient IDs.
    -   Supports concurrent runs.
    -   Allows running pipelines locally, via SSH, or in a SLURM cluster.
    -   Browse logs from pipeline runs that have failed.
-   **Pipeline Execution and Reruns**:
    -   Execute pipelines on selected study UIDs or patient IDs.
    -   Rerun pipelines with different code versions using Dagster tags.
    -   Provides CLI commands for running and backfilling pipeline runs.
-   **Automation with directory watching**:
    -   Employs Dagster sensors to watch a directory and automatically trigger analysis jobs.
-   **Post-Run Analysis**:
    -   Collects pipeline results into a structured directory hierarchy.
    -   Generates a runs table (`cartilage_thickness_runs.parquet`) for tracking execution details.
    -   Provides scripts and Jupyter notebooks for aggregating and visualizing results.
-   **Error Handling and Asset Checks**:
    -   Implements asset checks to identify and list failed or missing output results.
    -   Facilitates easy reruns of failed analyses by providing lists of partitions that require attention.

## ⚙️ Setup and execution

### 🐍 Pixi

You can install all the dependencies inside a reproducible software environment via pixi. To do that, [install pixi](https://pixi.sh), clone the repository, and run the following command from the root folder.

```bash
pixi install -a
```

To see all tasks available:

```bash
pixi task list
```

Start and access the [Dagster UI](http://127.0.0.1:3000) locally.

```bash
pixi run dev
```

To configure the host and port:

```bash
pixi run dev -h 0.0.0.0 -p 3001
```

### Setup OAI Cartilage Thickness Pipeline

We need to configure where the OAI source files live and how to run the [OAI Analysis 2](https://github.com/uncbiag/OAI_analysis_2) code. Configuration is pulled from environment variables. One way to set env vars is with a .env file in the root directory of this repo.

```bash
cp .env.example .env
```

#### Dagster home directory

Edit the .env file and set `DAGSTER_HOME` to the absolute path of the `knee-sarg/data/dagster_home` directory. Example: `DAGSTER_HOME=/home/paul/src/oai/knee-sarg/data/dagster_home`

This directory has a dagster.yaml file that limits the number of concurrent runs of the OAI analysis pipeline. The directory also holds the Dagster database which maintains runs/logs/etc state between `pixi run dev` calls.

#### OAI data directory and output directory

Edit the .env file.

-   Set `OAI_DATA_ROOT` to the directory where the source OAI files are at rest.
-   Set `FILE_STORAGE_ROOT` to a directory to store the pipeline output. The pipeline creates 2 directories here: ingested and collections.

#### OAI Analysis 2 setup

For a 2 concurrent runs of the pipeline, we need a computer with \~22 GB GPU memory.

```bash
git clone https://github.com/dzenanz/OAI_analysis_2.git
cd OAI_analysis_2
python -m virtualenv venv
source venv/bin/activate
pip install --upgrade pip
pip install -e .
pwd # note output for PIPELINE_SRC_DIR env var
```

Edit the .env file.

-   Set `PIPELINE_SRC_DIR` to the OAI_analysis_2 repo root directory
-   Set `ENV_SETUP_COMMAND` to shell commands that need to be run before Dagster calls `python ./oai_analysis/pipeline_cli.py {remote_image_path} {remote_out_dir}`.

If you followed the virtualenv and pip setup above, the env could be similar to this

```
PIPELINE_SRC_DIR=/home/paulhax/src/OAI_analysis_2 # pipeline repo dir
ENV_SETUP_COMMAND=. ./venv/bin/activate
```

##### Run via SSH

By default, Dagster runs the pipeline in a subprocess on the same computer it runs. If the computer that runs the pipeline is different than the Dagster one, make a user with SSH access on the pipeline running computer.

-   Set the `OAI_PIPELINE_RESOURCE` environment variable to `ssh`
-   Set `SSH_HOST`, `SSH_USERNAME`, `SSH_PASSWORD` environment variables when calling `pixi run dev`

##### Run via SLURM

-   Set the `OAI_PIPELINE_RESOURCE` environment variable to `slurm`
-   Set the `SBATCH_ARGS` environment variable to CLI args to pass to SLURM's sbatch program. I.e `SBATCH_ARGS=--partition=firstq`

### Run OAI Cartilage Thickness Pipeline

#### Create study_uids_to_run.json

Create a `study_uids_to_run.json` file in the `knee-sarg/data/oai-sampler` directory.
The JSON file should contain an array of OAI study UIDs. Example:

```json
[
    "1.3.6.1.4.1.21767.172.16.9.194.1169599504.4700.0",
    "1.3.6.1.4.1.21767.172.16.9.194.1169599504.4701.0"
]
```

The `data/oai-sampler/make_study_uids_json.py` script will make a `study_uids_to_run.json` file by
pulling from the study UIDs in `data/oai-sampler/study_uid_to_vol_path.csv`.

#### Materialize `oai_study_uids_to_run`

With Dagster running (`pixi run dev`,) materialize the `oai_study_uids_to_run` asset.

### Rerun Cartilage Thickness Pipeline

##### Using CLI

After study_uid partitions have been created (by materializing `oai_study_uids_to_run`),
you can rerun specific the pipeline on select `study_uid`s partitions via the Dagster CLI:

```bash
pixi run cartilage-thickness --partitions 1.3.12.2.1107.5.2.13.20576.4.0.8047887714483085,1.3.6.1.4.1.21767.172.16.11.7.1385496118.2.0
```

##### Compare results across code versions

The leaf output directory is the "code version". Set the output directory name with the `oai/code-version` tag

```bash
pixi run cartilage-thickness --tags '{"oai/code-version": "new-approach"}' --partitions 0.3.12.2.1107.5.2.13.20576.4.0.8047887714483085
```

To start another run using a different pipeline source directory, add the `oai/src-directory` tag.

```bash
pixi run cartilage-thickness --tags '{"oai/code-version": "old-approach", "oai/src-directory": "/home/paulhax/src/old-OAI_analysis_2"}' --partitions 1.3.12.2.1107.5.2.13.20576.4.0.8047887714483085'
```

There is a python script that backfills a given number of studies with given tags.

```bash
pixi run ct-backfill --count 2 --tags '{"oai/code-version": "old-approach", "oai/src-directory": "/home/paulhax/src/old-OAI_analysis_2"}'
```

#### Sample a OAI patient

This tests the cloud scan upload triggering cartilage thickness pipeline. For running with local access to OAI dataset, use the `oai_study_uids_to_run` method described above.

With Dagster running (`pixi run dev`), create a `patient_ids.json` file in the `knee-sarg/data/oai-sampler` directory. The JSON file should contain an array of OAI patient IDs. Example:

```json
["9000798", "9007827"]
```

There are example JSON files in the `data/oai-sampler` directory.  
Start the the `patient_id_sensor` and the `staged_study_sensor` sensors. The `patient_id_sensor` sensor checks the `patient_ids.json` file every 30 seconds and kicks off this automatic flow:

1. A new patient ID partition is created for each patient ID in the `patient_ids.json` file.
2. Asset OAI patient data is copied to `FILE_STORAGE_ROOT/staged`
3. A sensor checks every 30 seconds for new folders of studies `staged` and starts the ingest_and_analyze_study job and creates a `study_uid` partition.
4. ingest_study asset copies the study files to `FILE_STORAGE_ROOT/ingested`
5. The cartilage_thickness asset runs the OAI_analysis_2 pipeline and copies the output files into `FILE_STORAGE_ROOT/collections`

### Post Run Analysis

#### OAI Collection Directory structure

`FILE_STORAGE_ROOT/collections/oai/[patient id]/[study description-study UID]/cartilage_thickness/[code version]`

The study level directory, (study description-study UID,) holds `oai.json`. That file has
fields from the patient's matching record in the OAI dataset's `enrollee01.txt` file.

#### Pipeline Runs Table

The `collections/oai/cartilage_thickness_runs.parquet` file has a row for each completed run.  
Use the table to iterate through the pipeline results. The columns:

`patient_id  study_uid  series_uid  computed_files_dir  code_version`

#### Collect images in one directory

Puts all 2D images in a flat `collections/oai/all-images` directory.

To collect images, materialize the `collected_images_oai` asset or run this CLI command:

```bash
pixi run collect-images
```

#### View images in Jupyter Notebook

```bash
pixi run jupyter
```

Then open `scripts/cartilage_thickness_images.ipynb`

#### Collect study UIDs that failed

There are 2 Dagster asset checks can can be used to build lists of errored study UID partitions to rerun.
Run the asset check, then view it's metadata in Dagster UI to get list of `study_uid` partitions.

-   `has_current_code_version_output_oai` asset check lists `study_uid` partitions that don't have runs matching
    the current code version in the `CARTILAGE_THICKNESS_CODE_VERSION` env var.
-   `has_image_files` asset check sees if rows in `cartilage_thickness_runs.parquet` file are missing images in
    the output directories.

Knee-SARG is a fully open-source, local-first image analysis platform that uses Dagster for orchestrating image processing pipelines, specifically for cartilage thickness analysis on knee imaging data from the Osteoarthritis Initiative (OAI).

## 💡 Principles

-   **Open**: Code, standards, infrastructure, and data, are public and open source.
-   **Modular and Interoperable**: Each component can be replaced, extended, or removed. Works well in many environments (your laptop, in a cluster, or from the browser), can be deployed to many places (S3 + GH Pages, IPFS, ...) and integrates with multiple tools (thanks to the Arrow and Zarr ecosystems). Use open tools, standards, infrastructure, and share data in accessible formats.
-   **Data as Code**: Declarative stateless transformations tracked in `git`. Improves data access and empowers data scientists to conduct research and helps to guide community-driven analysis and decisions. Version your data as code! Publish and share your reusable models for others to build on top. Datasets should be both reproducible and accessible!
-   **Glue**: Be a bridge between tools and approaches. E.g: Use software engineering good practices like types, tests, materialized views, and more.
-   [**FAIR**](https://www.go-fair.org/fair-principles/).
-   **KISS**: Minimal and flexible. Rely on tools that do one thing and do it well.
-   **No vendor lock-in**
    -   Rely on Open code, standards, and infrastructure.
    -   Use the tool you want to create, explore, and consume the datasets. Agnostic of any tooling or infrastructure provider.
    -   Standard format for data and APIs! [Keep your data as future-friendly and future-proof as possible](https://indieweb.org/longevity)!
-   **Distributed**: Permissionless ecosystem and collaboration. Open source code and make it ready to be improved.
-   **Community**: that incentives contributors.
-   **Immutability**: Embrace idempotency. Rely on content-addressable storage and append-only logs.
-   **Stateless and serverless**: as much as possible. E.g. use GitHub Pages, host datasets on S3, interface with HTML, JavaScript, and WASM. No servers to maintain, no databases to manage, no infrastructure to worry about. Keep infrastructure management lean.
-   **Offline-first**: Rely on static files and offline-first tools.
-   **Above all, have fun and enjoy the process** 🎉

## 👏 Acknowledgements

-   This project was built on the principles espoused by David Gasquez at [Datonic](https://datonic.io). It is built on the approach in the [Datadex](https://datadex.datonic.io/) Open Data Platform and extended for scientific imaging data with [OME-Zarr](https://ngff.openmicroscopy.org/) and the DICOM-based image data model in the [NIH Imaging Data Commons](https://portal.imaging.datacommons.cancer.gov/).
-   Knee-SARG is possible thanks to amazing open source projects like [DuckDB](https://www.duckdb.org/), [dbt](https://getdbt.com), [Dagster](https://dagster.io/), [ITK](https://docs.itk.org) and many others...
