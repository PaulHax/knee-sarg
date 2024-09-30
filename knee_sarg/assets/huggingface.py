from dagster import asset

from ..resources import CollectionPublisher, OAI_COLLECTION_NAME


def create_hf_asset(collection_name: str, collection_description: str):
    @asset(name=f"huggingface_{collection_name}")
    def hf_asset(collection_publisher: CollectionPublisher) -> None:
        """
        Upload collection to HuggingFace.
        """

        readme_content = f"""
---
license: mit
---
# Knee-SARG {collection_name} collection

[Knee-SARG](https://github.com/open-radiogenomics/knee-sarg) is a fully open-source and local-first platform
that improves how communities collaborate on open data to diagnose knee cancer and perform epidemiology.

{collection_description}
        """

        collection_publisher.publish(
            collection_name=collection_name,
            readme=readme_content,
            generate_data_package=True,
        )

    return hf_asset


collections = [
    (
        OAI_COLLECTION_NAME,
        """
## OAI Radiogenomics

Source: The Osteoarthritis Initiative, https://nda.nih.gov/oai

> The Osteoarthritis Initiative (OAI) is a multi-center, ten-year observational study of men and women, sponsored by the National Institutes of Health (part of the Department of Health and Human Services). The goals of the OAI are to provide resources to enable a better understanding of prevention and treatment of knee osteoarthritis, one of the most common causes of disability in adults.
""",
    ),
]

assets = []
for collection_name, collection_description in collections:
    a = create_hf_asset(collection_name, collection_description)
    assets.append(a)
