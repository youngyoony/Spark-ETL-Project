#!/bin/bash

# download-data.sh
python3 -c "
import fetch_data

market_trends = fetch_data.fetch_market_trends()
fetch_data.save_data_to_file(market_trends, 'jobs/data/market_trends.json')

stock_time_series = fetch_data.fetch_stock_time_series()
fetch_data.save_data_to_file(stock_time_series, 'jobs/data/stock_time_series.json')
"