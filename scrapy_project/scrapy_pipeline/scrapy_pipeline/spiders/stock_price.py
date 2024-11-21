import scrapy
import json
import pandas as pd
import os

# Generate your API key from https://www.alphavantage.co/support/#api-key and Replace with your Alpha Vantage API key in .env file
api_key = os.getenv("API_KEY")


class StockSpider(scrapy.Spider):
    name = "alpha_vantage_stock_spider"

    # The stock symbol for which you want to get the data
    symbol = "NVDA"
    """
    Alpha Vantage API provides stock price data for the next day of trading.
    We are collecting the entire day's data at a 1-minute interval to analyze price movements.
    This data will be used for intraday analysis or for other trading strategies.
    """

    base_url = "https://www.alphavantage.co/query"

    def start_requests(self):
        # Initial request to fetch stock data
        url = self.build_url(self.symbol)
        yield scrapy.Request(url, callback=self.parse)

    def build_url(self, symbol):
        """Builds the request URL with the correct parameters"""
        return f"{self.base_url}?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={api_key}"

    def parse(self, response):
        """Parse the stock data from the JSON response"""
        data = json.loads(response.text)

        if "Error Message" in data:
            self.logger.error(f"API Error: {data['Error Message']}")
            return

        if "Information" in data:
            self.logger.error(f"API information: {data['Information']}")
            return

        time_series = data.get("Time Series (1min)", {})

        if not time_series:
            self.logger.warning(f"No data returned for symbol {self.symbol}.")
            return
        stock_price_list = []
        for time_stamp, stock_price in time_series.items():
            data = {
                "time": time_stamp,
                "open": stock_price["1. open"],
                "high": stock_price["2. high"],
                "low": stock_price["3. low"],
                "close": stock_price["4. close"],
                "volume": stock_price["5. volume"],
            }
            stock_price_list.append(data)

        df = pd.DataFrame(stock_price_list)
        df["symbol"] = self.symbol

        # save csv file to local
        df.to_csv("../../alpha_vantage_stock_price.csv")
