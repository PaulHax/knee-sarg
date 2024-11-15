#!/bin/bash

# Can set the SBATCH_ARGS environment variable to pass arguments to sbatch, e.g.:
# SBATCH_ARGS="--partition gpu --qos gpu_access --gres=gpu:1"
# See the .env.example file

# The name of the job:
##SBATCH --job-name="oai_run_analysis"

#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16g

# The maximum running time of the job in days-hours:mins:sec
#SBATCH --time=0-00:10:00

# compute partition
##SBATCH --qos gpu_access 
##SBATCH --partition firstq
##SBATCH --gres=gpu:1  


# check that the script is launched with sbatch
if [ "x$SLURM_JOB_ID" == "x" ]; then
   echo "You need to submit your job to the queuing system with sbatch"
   exit 1
fi

# CLI args
image_path=$1
output_dir=$2
laterality=$3
oai_src_dir=$4

if [ -z "$image_path" ] || [ -z "$output_dir" ] || [ -z "$laterality" ] || [ -z "$oai_src_dir" ]; then
    echo "Usage: $0 <image_path> <output_dir> <laterality> <oai_src_dir>"
    exit 1
fi

cd "$oai_src_dir" || exit 1

# The job command(s):
eval "$ENV_SETUP_COMMAND"
python ./oai_analysis/pipeline_cli.py "$image_path" "$output_dir" "$laterality"


