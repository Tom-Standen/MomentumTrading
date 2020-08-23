"""
File to hold all functions that access Binance.com via API

"""

import datetime
import os


def get_historical_klines(client, instrument, granularity, start_date):
    """
    Take historical klines from binance api and save as an output file
    instrument: str - currency pair eg BTC_USDT
    granularity: str - the time interval between klines, eg 1m, 1h, 6h, 1d, 1M - see api docs for more
    start_date: datetime - the start_date 
    
      
    From Binance API Docs - get hist klines output

    [
        1499040000000,      // Open time
        "0.01634790",       // Open
        "0.80000000",       // High
        "0.01575800",       // Low
        "0.01577100",       // Close
        "148976.11427815",  // Volume
        1499644799999,      // Close time
        "2434.19055334",    // Quote asset volume
        308,                // Number of trades
        "1756.87402397",    // Taker buy base asset volume
        "28.46694368",      // Taker buy quote asset volume
        "17928899.62484339" // Ignore.
      ]
    
    
    """
    # get the data in generator form from the api
    kline_gen = client.get_historical_klines_generator(symbol=instrument, interval=granularity,
                                                       start_str=start_date.strftime("%d %b, %Y"))
    # name of file
    datafile = "data/klines/live.out".format(instrument, granularity, start_date.strftime("%Y-%m-%d"))
    # write to file
    with open(datafile, "w") as file:
        n = 0
        # columns
        file.write("time,open,high,low,close,volume,num_trades,qav,taker_buy_bav,taker_buy_qav")
        file.write("\n")
        for kline in kline_gen:
            n += 1
            # write out each row to the file
            try:
                # change the time from timestamp to a datetime
                ctimestamp = kline[6]
                c_datetime = datetime.datetime.fromtimestamp(ctimestamp / 1000)
                rec = "{cdatetime},{o},{h},{l},{c},{v},{num},{qav},{taker_bav},{taker_qav}".format(
                    cdatetime=c_datetime,
                    o=kline[1],
                    h=kline[2],
                    l=kline[3],
                    c=kline[4],
                    v=kline[5],
                    num=kline[8],
                    qav=kline[7],
                    taker_bav=kline[9],
                    taker_qav=kline[10]
                )
            except Exception as e:
                print(e, file)
            else:
                file.write(rec + "\n")

        print(f"{n} records saved to {datafile}")
    return None


# truncate candle_df into one of the three timeframes and save it
def save_candle_df_timeframe(candle_df, tf, symbol, gran,
                             data_base_path=r'C:\Users\User\Documents\algo-trading\binance\data\klines'):
    """
    Filter down candlestick data to specific timeframes
    args:
    candle_df
    tf- int: 1,2,3 corresponding to 2017-01 - 2018-06, 2018-06 - 2019-06 and 2019-06 - 2020-06
    symbol: the crypto pair
    gran: the grnaularity of the data daily(1d), hourly (1h) etc..
    
    """
    if tf == 1:
        df = candle_df[candle_df['time'] < "2018-06"]
        date_str = "2017-09_2018-06"
    elif tf == 2:
        df = candle_df[(candle_df['time'] >= "2018-06") & (candle_df['time'] < "2019-06")]
        date_str = "2018-06_2019-06"
    elif tf == 3:
        df = candle_df[candle_df['time'] >= "2019-06"]
        date_str = "2019-06_2020-06"
    else:
        print('Incorrect value for timeframe, enter 1,2 or 3')
        return False

    new_dir_path = data_base_path + f'/{symbol}/{date_str}'
    if not os.path.isdir(new_dir_path):
        os.makedirs(new_dir_path)

    df.to_csv(new_dir_path + f'/{symbol}_{gran}_{date_str}.out')
    return None

