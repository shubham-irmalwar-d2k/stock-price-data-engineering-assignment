from dagster import (
    AssetSelection,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from .partitions import (
    daily_partitions,
)

daily_alpha_vantage_stock_price_job_schedule = build_schedule_from_partitioned_job(
    define_asset_job(
        "daily_alpha_vantage_stock_price_job",
        selection=AssetSelection.key_prefixes(["share_price", "alpha_vantage"]),
        partitions_def=daily_partitions,
    )
)
