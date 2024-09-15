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

def fetch_app_details():
    url = "https://store-apps.p.rapidapi.com/app-details"
    querystring = {"app_id":"com.google.android.apps.subscriptions.red","region":"us","language":"en"}
    headers = {
        "x-rapidapi-key": "7c8a2c6a79msh04674dc52dbd72dp125f50jsn20be7524e794",
        "x-rapidapi-host": "store-apps.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    return response.json()

def fetch_top_grossing_apps():
    url = "https://store-apps.p.rapidapi.com/top-grossing-apps"
    querystring = {"limit":"200","region":"us","language":"en"}
    headers = {
        "x-rapidapi-key": "7c8a2c6a79msh04674dc52dbd72dp125f50jsn20be7524e794",
        "x-rapidapi-host": "store-apps.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    return response.json()

def fetch_top_paid_apps():
    url = "https://store-apps.p.rapidapi.com/top-paid-apps"
    querystring = {"limit":"50","region":"us","language":"en"}
    headers = {
        "x-rapidapi-key": "7c8a2c6a79msh04674dc52dbd72dp125f50jsn20be7524e794",
        "x-rapidapi-host": "store-apps.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    return response.json()

def fetch_top_free_apps():
    url = "https://store-apps.p.rapidapi.com/top-free-apps"
    querystring = {"limit":"50","region":"us","language":"en"}
    headers = {
        "x-rapidapi-key": "7c8a2c6a79msh04674dc52dbd72dp125f50jsn20be7524e794",
        "x-rapidapi-host": "store-apps.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    return response.json()

def save_data_to_file(data, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(data, f)

if __name__ == "__main__":
    market_trends = fetch_market_trends()
    save_data_to_file(market_trends, 'data/market_trends.json')

    stock_time_series = fetch_stock_time_series()
    save_data_to_file(stock_time_series, 'data/stock_time_series.json')

    app_details = fetch_app_details()
    save_data_to_file(app_details, 'data/app_details.json')

    top_grossing_apps = fetch_top_grossing_apps()
    save_data_to_file(top_grossing_apps, 'data/top_grossing_apps.json')

    top_paid_apps = fetch_top_paid_apps()
    save_data_to_file(top_paid_apps, 'data/top_paid_apps.json')

    top_free_apps = fetch_top_free_apps()
    save_data_to_file(top_free_apps, 'data/top_free_apps.json')