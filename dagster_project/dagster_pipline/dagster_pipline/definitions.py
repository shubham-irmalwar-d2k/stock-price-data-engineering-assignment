from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_pipline.resources import (
    S3BucketResource,
)
from dagster_pipline.utils.dagster import load_jobs_from_modules

from . import assets, jobs
from .jobs import (
    daily_alpha_vantage_stock_price_job_schedule,
)

all_assets = load_assets_from_modules([assets])
all_jobs = load_jobs_from_modules([jobs])
all_schedules = [
    daily_alpha_vantage_stock_price_job_schedule,
]

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources={
        "rawdata_bucket": S3BucketResource(
            endpoint_url=EnvVar("MINIO_ENDPOINT_URL"),
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
            bucket_name=EnvVar("MINIO_DATA_BUCKET"),
            path_prefix=EnvVar("RAW_DATA_PREFIX"),
        ),
    },
)
