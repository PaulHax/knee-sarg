import os
import shutil
import argparse
from dotenv import load_dotenv
from cartilage_thickness_collection import (
    get_oai_collection_dir,
    get_runs,
    THICKNESS_IMAGES,
)


def collect_images(output_dir, image_files, max_runs):
    runs = get_runs()[:max_runs] if max_runs else get_runs()

    os.makedirs(output_dir, exist_ok=True)

    for _, run in runs.iterrows():
        patient_id = run["patient_id"]
        computed_dir = run["computed_files_dir"]
        study_description = computed_dir.split("/")[-3].split("-")[0]

        # study description examples: OAI^MR^12 MONTH^LEFT OAI^MR^24 MONTH^RIGHT OAI^MR^ENROLLMENT^LEFT
        is_left = study_description.find("LEFT") > -1
        laterality = "left" if is_left else "right"

        month = study_description.split("^")[2]
        if month == "ENROLLMENT":
            month = "00 MONTH"

        for image in image_files:
            src_path = os.path.join(computed_dir, image)
            if os.path.exists(src_path):
                filename, extension = os.path.splitext(image)
                code_version = run["code_version"]
                new_filename = f"{code_version}-{filename}-{patient_id}-{laterality}-{month}{extension}"
                dest_path = os.path.join(output_dir, new_filename)
                shutil.copy(src_path, dest_path)
            else:
                print(f"File not found: {src_path}")


if __name__ == "__main__":
    load_dotenv()

    output_dir_default = str(get_oai_collection_dir() / "all-images")

    parser = argparse.ArgumentParser(description="Collect images from runs.")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=output_dir_default,
        help=f"Directory to save collected images. Default: {output_dir_default}",
    )
    parser.add_argument(
        "--images",
        nargs="+",
        default=THICKNESS_IMAGES,
        help="List of image files to collect.",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=None,
        help="Maximum number of runs to process. Default: all runs",
    )

    args = parser.parse_args()

    collect_images(args.output_dir, args.images, args.max_runs)
