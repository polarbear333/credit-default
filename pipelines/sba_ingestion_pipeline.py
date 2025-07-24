from zenml import pipeline

from pipelines.steps.ingestion import fetch_and_combine_sba_data
from pipelines.steps.storage import write_to_postgres


@pipeline(
    enable_cache=True,
    settings={
        "orchestrator.airflow": {
            "dag_kwargs": {
                "schedule": "@daily",  # airflow's cron syntax
                "catchup": False,
                "tags": ["ingestion", "sba"],
            }
        }
    },
)
def sba_ingestion_pipeline():
    """defines the end-to-end data ingestion pipeline."""
    sba_dataframe = fetch_and_combine_sba_data()
    write_to_postgres(sba_dataframe)


if __name__ == "__main__":
    sba_ingestion_pipeline()
