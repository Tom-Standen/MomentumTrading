#!/home/standentrading/ethusdt_frama/venv/bin/python3
"""
 10/06/2020
  - Use Binance's API to get the latest 2hourly kline (candlestick) for ETH vs USDT
  - Read in historical kline data
  - Calculate the new fractal moving averages of the chosen pairs.
 At time of writing the 12 chosen pairs are:
 5 vs 202, 5 vs 204, 6 vs 203
 9 vs 204
 11 vs 206, 12 vs 206, 13 vs 206
 13 vs 218, 14 vs 219, 15 vs 220
 25 vs 220
 45 vs 220
  - Deduce whether there has been a change in position,
  that is whether there has been a cross in any of the MA pairs
  - If so, execute a trade for the relevant MA pair. If not, close the program, re-run in 2 hours time
"""
import logging
import datetime
import sys
import os

import pandas as pd
# import external modules
# unofficial python wrapper for binance api
from binance.client import Client

# import personal modules
from api_keys import trading_api
# function to calculate fractal moving averages
from functions.auto_trading_functions import frama_pos_check, get_new_trades,\
    load_trade_history, get_buy_and_sell_quantities, interpret_order_response
from functions.api_functions import get_historical_klines

run_datetime = datetime.datetime.now()
kline_start_datetime = run_datetime - datetime.timedelta(hours=600)

# global variables
data_base_path = "data"
no_run = False
if __name__ == "__main__":
    if no_run:
        sys.exit(1)
    # Create a log file to record the output of the file
    log_file_path: str = 'log'
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        filename=log_file_path+'/{}_trades.log'.format(run_datetime.strftime("%Y-%m-%d")), filemode='a', level=logging.INFO)
    logging.info("-------------------------------------------------------------")
    # Connect to API
    try:
        # set connection
        client = Client(api_key=trading_api['api_key'], api_secret=trading_api['secret_key'])
    except Exception as e:
        logging.error(e)
        logging.info('Failed when creating the API connection')
        sys.exit()
    # Retrieve Live Kline data and load in the data
    try:
        # parameters used to retrieve specific klines from binace API
        # only need ~ 30 klines more than the MA to guarantee accuracy 30 + 220 = 250 - go 300 to be safe
        # 300 * 2h = 600 hrs ; start_datetime = datetime.now() - timedelta(hours=600)
        params = dict(symbol='ETHUSDT', granularity='2h', start_datetime=kline_start_datetime)
        get_historical_klines(client, instrument=params['symbol'], granularity=params['granularity'],
                              start_date=params['start_datetime'])

        kline_df = pd.read_csv(data_base_path + '/klines/live.out')
        trigger_price = kline_df.loc[kline_df.index[-1]]['close']
        logging.info('The latest price is {}'.format(trigger_price))
    except Exception as e:
        logging.error(e)
        logging.info('Connection made but issue retrieving or loading data')
        sys.exit()

    # list of tuples for the ma pairs under investigation
    ma_pairs = [(5, 202), (5, 204), (6, 203,), (9, 204), (11, 206), (12, 206), (13, 206), (13, 218), (14, 219),
                (15, 220), (25, 220), (45, 220)]
    new_positions = frama_pos_check(kline_df, ma_pairs)
    trade_hist_dict = load_trade_history(new_positions)
    new_trades = get_new_trades(new_positions, trade_hist_dict)
    logging.info('FRAMA position for each ma pair: \n {}'.format(new_positions))
    if new_trades:
        # only continue here if there are trades to be made
        logging.info('Trades to execute: \n {}'.format(new_trades))
        usdt_qty, eth_qty = get_buy_and_sell_quantities(new_trades, trade_hist_dict)
        logging.info('ETH to sell: {}, USDT to sell: {}'.format(eth_qty, usdt_qty))
        # place orders
        # buy
        if usdt_qty:
            buy_order = client.create_order(
                symbol=params['symbol'],
                side='BUY',
                # timeInForce='GTC',  # good til cancelled - doesn't apply in market orders
                type='MARKET',  # market or limit
                quoteOrderQty=usdt_qty,  # amount of crypto to buy in USDT
            )
            logging.info('BUY ORDER: \n {}'.format(buy_order))
            # get details from order response
            order_id, order_time, eth_order, usdt_order, comm, comm_asset, av_price  = interpret_order_response(buy_order)
            # udpate the trade log
            for key, pos in new_trades.items():
                if pos == 1:
                    # buy signal
                    usdt_to_sell = trade_hist_dict[key].loc[trade_hist_dict[key].index[-1]]['usdt_held']  # the amount this particular pair 'owned'
                    pair_fraction = usdt_to_sell / (usdt_order)  # the fraction of the trade belonging to this pair
                    #eth_bought
                    eth_bought = (eth_order - comm) * pair_fraction
                    comm_frac = comm * pair_fraction
                    # log in case of error
                    logging.info('{}: eth_bought={}, av_price={}, comm={}'.format(key, eth_bought, av_price, comm_frac))
                    # write a new entry to the dataframe
                    trade_hist_dict[key].loc[order_id] = [order_time, eth_bought, 0, av_price, trigger_price, comm_frac, comm_asset]
        # sell
        if eth_qty:
            sell_order = client.create_order(
                symbol=params['symbol'],
                side='SELL',
                # timeInForce='GTC',  # good til cancelled - doesn't apply in market orders
                type='MARKET',  # market or limit
                quantity=eth_qty,  # amount of ETH to sell
            )
            # get details from order response
            order_id, order_time, eth_order, usdt_order, comm, comm_asset, av_price = interpret_order_response(sell_order)
            logging.info('SELL ORDER: \n {}'.format(sell_order))
            # update trade history csvs
            for key, pos in new_trades.items():
                if pos == -1:
                    # sell signal
                    eth_to_sell = trade_hist_dict[key].loc[trade_hist_dict[key].index[-1]]['eth_held'] # amount of eth held by the pair
                    pair_fraction = eth_to_sell / eth_order
                    usdt_bought = (usdt_order - comm) * pair_fraction
                    comm_frac = comm * pair_fraction
                    # log in case of error
                    logging.info('{}: usdt_bought={}, av_price={}, comm={}'.format(key, usdt_bought, av_price, comm_frac))
                    # write a new entry into the dataframe
                    trade_hist_dict[key].loc[order_id] = [order_time, 0, usdt_bought, av_price,
                                                         trigger_price, comm_frac, comm_asset]

        # save the new trade logs
        for key, df in trade_hist_dict.items():
            df.to_csv(data_base_path + '/trade_history/eth_pairs/portfolio_' + key + '.csv')
    else:
        logging.info('No new trades')
        sys.exit()


