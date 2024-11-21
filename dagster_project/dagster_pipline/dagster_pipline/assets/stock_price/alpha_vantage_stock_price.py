import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os
from io import BytesIO
import subprocess
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)
import s3fs
from dagster_pipline.resources import (
    S3BucketResource,
)

bucket_name = os.getenv("MINIO_DATA_BUCKET")
KEY_PREFIX = ["alpha_vantage", "stock_price"]


@asset(
    key_prefix=KEY_PREFIX,
    compute_kind="Scrapy",
)
def daily_crawl_alpha_vantage_stock_price(
    context: AssetExecutionContext,
) -> MaterializeResult:
    scrapy_command = ["scrapy", "crawl", "alpha_vantage_stock_spider"]

    try:
        subprocess.run(scrapy_command, cwd="../../scrapy_project/scrapy_pipeline")
        context.log.info("Scrapy spider completed. Output saved to output.json")

    except subprocess.CalledProcessError as e:
        context.log.error(f"Error running Scrapy spider: {e.stderr}")
        raise


@asset(
    key_prefix=KEY_PREFIX,
    deps=[daily_crawl_alpha_vantage_stock_price],
    compute_kind="Data Movement",
)
def daily_data_movement_alpha_vantage_stock_price(
    context: AssetExecutionContext,
    rawdata_bucket: S3BucketResource,
) -> MaterializeResult:
    df = pd.read_csv("../../alpha_vantage_stock_price.csv")
    if df.empty:
        context.log.info("Empty dataframe found")
        return MaterializeResult(
            metadata={
                "num_records": len(df),
                "preview": MetadataValue.md(df.head().to_markdown()),
            },
        )

    """clean data as per requirement
    split time column into stock_date and stock_time columns
    """
    del df["Unnamed: 0"]
    df["stock_date"] = df["time"].apply(lambda x: x.split(" ")[0])
    df["stock_time"] = df["time"].apply(lambda x: x.split(" ")[1])
    df["stock_date"] = (pd.to_datetime(df["stock_date"])).dt.strftime("%Y-%m-%d")

    stock_data_date = df["stock_date"][0]
    raw_file_minio_uri = rawdata_bucket.build_uri(
        [
            "alpha_vantage_stock_price_data",
            f"date={stock_data_date}",
            "stock_price.parquet",
        ]
    )
    df.to_parquet(
        raw_file_minio_uri,
        storage_options=rawdata_bucket.storage_options,
        index=False,
    )

    context.log.info(f"Inserted dataframe into minio: {raw_file_minio_uri}")
    return MaterializeResult(
        metadata={
            "minio_key": raw_file_minio_uri,
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        },
    )


@asset(
    key_prefix=KEY_PREFIX,
    compute_kind="Data Analysis",
)
def daily_data_analysis_alpha_vantage_stock_price(
    context: AssetExecutionContext,
    rawdata_bucket: S3BucketResource,
) -> MaterializeResult:
    # df = pd.read_parquet("crismac-dl-raw-bucket-dev/alpha_vantage_stock_price_data/date=2024-11-14/stock_price", engine='pyarrow', storage_options=rawdata_bucket)

    fs = s3fs.S3FileSystem(
        key=rawdata_bucket.storage_options["key"],
        secret=rawdata_bucket.storage_options["secret"],
        client_kwargs={"endpoint_url": rawdata_bucket.storage_options["endpoint_url"]},
    )

    for i in range(1, 5):  # collect latest file from minio
        stock_date = (datetime.today() - timedelta(days=i)).strftime("%Y-%m-%d")
        file_key = (
            f"alpha_vantage_stock_price_data/date={stock_date}/stock_price.parquet"
        )

        # Construct the s3 URI for the Parquet file
        minio_uri = f"{bucket_name}/{file_key}"

        # Open the Parquet file directly with pandas using s3fs
        try:
            with fs.open(minio_uri, "rb") as f:
                df = pd.read_parquet(f, engine="pyarrow")
        except Exception as e:
            df = pd.DataFrame()
            context.log.error(f"Error file not found on {stock_date}: {e}")
        if len(df):
            break

    figure_date = df["stock_date"][0]
    symbol = df["symbol"][0]

    # Plot the stock price trends
    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df["close"], label="Closing Price", color="blue", lw=2)
    plt.title(f"{symbol} Stock Price Trends ({figure_date})")
    plt.xlabel("Time")
    plt.ylabel("Price ($)")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    price_plot_io = BytesIO()
    plt.savefig(price_plot_io, format="png")
    price_plot_io.seek(0)  # Go to the beginning of the BytesIO object
    plt.close()
    # set path for price plot
    minio_price_plot_uri = f"{bucket_name}/alpha_vantage_stock_price_data/date={figure_date}/price_plot.png"
    # Upload the image to MinIO
    with fs.open(minio_price_plot_uri, "wb") as f:
        f.write(price_plot_io.read())

    context.log.info(f"Inserted price plot into minio: {minio_price_plot_uri}")
    # Bar chart for trading volume
    plt.figure(figsize=(12, 6))
    plt.bar(df.index, df["volume"], color="orange", alpha=0.7)
    plt.title(f"{symbol} Trading Volume ({figure_date})")
    plt.xlabel("Time")
    plt.ylabel("Volume")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    # Save plot to a BytesIO object (in-memory)
    volume_plot_io = BytesIO()
    plt.savefig(volume_plot_io, format="png")
    volume_plot_io.seek(0)  # Go to the beginning of the BytesIO object
    plt.close()

    minio_volume_plot_uri = f"{bucket_name}/alpha_vantage_stock_price_data/date={figure_date}/volume_plot.png"
    # Upload the image to MinIO
    with fs.open(minio_volume_plot_uri, "wb") as f:
        f.write(volume_plot_io.read())

    context.log.info(f"Inserted volume plot into minio: {minio_volume_plot_uri}")
