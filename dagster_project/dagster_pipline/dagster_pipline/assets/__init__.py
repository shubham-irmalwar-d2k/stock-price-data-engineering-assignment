from dagster import load_assets_from_package_module
from . import stock_price


STOCK_PRICE = "stock_price"
stock_price_assets = load_assets_from_package_module(
    package_module=stock_price, group_name=STOCK_PRICE
)
