import requests
import json
import os

def fetch_market_trends():
    url = "https://real-time-finance-data.p.rapidapi.com/market-trends"
    querystring = {"trend_type":"MARKET_INDEXES","country":"us","language":"en"}
    headers = {
        "x-rapidapi-key": "7c8a2c6a79msh04674dc52dbd72dp125f50jsn20be7524e794",
        "x-rapidapi-host": "real-time-finance-data.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    return response.json()

def fetch_stock_time_series():
    url = "https://real-time-finance-data.p.rapidapi.com/stock-time-series"
    querystring = {"symbol":"AAPL:NASDAQ","period":"1D","language":"en"}
    headers = {
        "x-rapidapi-key": "7c8a2c6a79msh04674dc52dbd72dp125f50jsn20be7524e794",
        "x-rapidapi-host": "real-time-finance-data.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    return response.json()

def save_data_to_file(data, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)  # 디렉토리 생성
    with open(filename, 'w') as f:
        json.dump(data, f)

if __name__ == "__main__":
    market_trends = fetch_market_trends()
    save_data_to_file(market_trends["data"]["trends"], '/opt/bitnami/spark/data/market_trends.json')

    stock_time_series = fetch_stock_time_series()
    save_data_to_file(stock_time_series["data"], '/opt/bitnami/spark/data/stock_time_series.json')