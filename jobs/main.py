import fetch_data
import process_data
import analyze_data

def main():
    # Fetch
    market_trends = fetch_data.fetch_market_trends()
    fetch_data.save_data_to_file(market_trends, 'data/market_trends.json')

    stock_time_series = fetch_data.fetch_stock_time_series()
    fetch_data.save_data_to_file(stock_time_series, 'data/stock_time_series.json')

    # Process
    spark = process_data.SparkSession.builder.appName("FinanceDataProcessing").getOrCreate()

    market_trends = process_data.load_data(spark, "data/market_trends.json")
    processed_market_trends = process_data.process_market_trends(market_trends)
    process_data.save_to_elasticsearch(processed_market_trends, "market_trends")

    stock_time_series = process_data.load_data(spark, "data/stock_time_series.json")
    processed_stock_time_series = process_data.process_stock_time_series(stock_time_series)
    process_data.save_to_elasticsearch(processed_stock_time_series, "stock_time_series")

    # Analyze
    market_trends = analyze_data.load_data_from_elasticsearch(spark, "market_trends")
    top_gainers, top_losers = analyze_data.analyze_market_trends(market_trends)
    analyze_data.save_analysis_result(top_gainers, "data/top_gainers.csv")
    analyze_data.save_analysis_result(top_losers, "data/top_losers.csv")

    stock_time_series = analyze_data.load_data_from_elasticsearch(spark, "stock_time_series")
    price_analysis, volume_analysis = analyze_data.analyze_stock_time_series(stock_time_series)
    analyze_data.save_analysis_result(price_analysis, "data/price_analysis.csv")
    analyze_data.save_analysis_result(volume_analysis, "data/volume_analysis.csv")

if __name__ == "__main__":
    main()