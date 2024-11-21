from dagster import DailyPartitionsDefinition

from dagster_pipline.utils import timezone

daily_partitions = DailyPartitionsDefinition(
    start_date="2024-11-01", timezone=timezone.IST.zone, fmt="%Y-%m-%d"
)
"""Daily partitions for T-1 day partition."""
