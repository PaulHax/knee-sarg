import polars as pl
from dagster import AssetIn, asset

from ..resources import DatasetPublisher


def create_hf_asset(dataset_name: str):
    @asset(name="huggingface_" + dataset_name, ins={"data": AssetIn(dataset_name)})
    def hf_asset(data: pl.DataFrame, dp: DatasetPublisher) -> None:
        """
        Upload data to HuggingFace.
        """

        readme_content = f"""
---
license: mit
---
# {dataset_name}

This dataset is produced and published automatically by [Datadex](https://github.com/davidgasquez/datadex),
a fully open-source, serverless, and local-first Data Platform that improves how communities collaborate on Open Data.

## Dataset Details

- **Number of rows:** {data.shape[0]}
- **Number of columns:** {data.shape[1]}
        """

        dp.publish(
            dataset=data,
            dataset_name=dataset_name,
            readme=readme_content,
            generate_datapackage=True,
        )

    return hf_asset


datasets = [
    "spain_energy_demand",
    "wikidata_asteroids",
    "threatened_animal_species",
    "country_year_indicators",
    "spain_ipc",
    "spain_aemet_historical_weather",
    "spain_water_reservoirs_data",
]

assets = []
for dataset in datasets:
    a = create_hf_asset(dataset)
    assets.append(a)
