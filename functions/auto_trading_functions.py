'''
Functions used in the auto trading scripts
'''

import sys
import numpy as np
import pandas as pd
import datetime
from functions.exploratory_functions import frama_perf

# global variables
data_base_path = "data"


def frama_pos_check(candle_df, ma_tuples):
    """
    Work out whether their has been a crossing of the moving average pairs in the last two hours

    :param candle_df: a historic candlestick dataframe retrieved via API with 'close' column
    :param ma_tuples: list of tuples of moving-average pairs
    :return: pos_dict: a dictionary of the pairs to trade and in which direction to trade
    """
    pos_dict = {}

    for fc, sc in ma_tuples:
        # naming convention
        long = sc
        short = fc

        # use GitHub function frama_perf to calculate frama
        candle_df['ma_' + str(short)] = frama_perf(candle_df['close'], fc)
        candle_df['ma_' + str(long)] = frama_perf(candle_df['close'], long)

        # string for column naming
        short_long = str(short) + '_' + str(long)
        # calculate whether we are long or short - long if rolling_15 > rolling_100 and vice versa
        candle_df['position_' + str(short_long)] = np.sign(candle_df['ma_' + str(short)] - candle_df['ma_' + str(long)])

        # find the latest position for each pair
        new_pos = candle_df.loc[candle_df.index[-1]]['position_' + str(short_long)]
        pos_dict[short_long] = new_pos
        # print(long, short, candle_df.loc[candle_df.index[-1]]['ma_' + str(long)], candle_df.loc[candle_df.index[-1]]['ma_' + str(short)])

    return pos_dict


def load_trade_history(position_dict):
    '''

    :param position_dict:
    :return: trade_hist_dict: a dictionary of pandas dfs containing the trading history for each pair
    '''
    trade_hist_dict = {}
    for key, new_pos in position_dict.items():
        # load in the trade history of the pairs with new trades to execute
        trade_hist_dict[key] = pd.read_csv(
            data_base_path + '/trade_history/eth_pairs/portfolio_' + key + '.csv', index_col='order_id')
    return trade_hist_dict


def get_new_trades(position_dict: object, trade_hist_dict: object) -> object:
    """
    Check the latest positions against portfolio positions and return a list of pairs that have changed
    :param position_dict: the latest positions as a dicitionary: key is the pair, value is +/-1 for long or short
    :return: new_trades - dict: the keys are the pairs and values is direction of new trade
    """
    # compare the existing positions against the latest positions to decide whether a trade is needed
    new_trades = {}
    for key, new_pos in position_dict.items():
        # if the portfolio has eth we are long if not we are short
        old_pos = +1 if trade_hist_dict[key].loc[trade_hist_dict[key].index[-1]]['eth_held'] > 0 else -1
        if new_pos != old_pos:
            # position has changed since last run
            new_trades[key] = new_pos
    return new_trades


def get_buy_and_sell_quantities(new_trades, trade_hist_dict):
    """
    :param new_trades - dict: key: ma_pair, value: buy(1) or sell(-1)
    :param trade_hist_dict - dict: key: ma_pair, value: df of trade history
    :return: usdt_qty, eth_qty float: the amount of usdt and eth to sell
    """
    usdt_qty = 0
    eth_qty = 0
    for key, pos in new_trades.items():
        # establish how much to eth to buy (usdt to sell) and eth to sell for each pair
        if pos == 1:
            # bull signal
            usdt_qty += trade_hist_dict[key].loc[trade_hist_dict[key].index[-1]]['usdt_held']
        elif pos == -1:
            # bear signal
            eth_qty += trade_hist_dict[key].loc[trade_hist_dict[key].index[-1]]['eth_held']
        else:
            # invalid signal
            print('Unkown buy/sell side signal')
            return False
    # round the quantities so that they can be accepted by the API
    usdt_qty = round(usdt_qty, 5)
    eth_qty = round(eth_qty, 5)
    return usdt_qty, eth_qty


def interpret_order_response(order: object) -> object:
    """
    Work out the average buy/sell price, the commission and the time at which the order was executed
    from the FULL API response on a market order
    :param order: the API response (FULL) object from binance
    :return order_id: the unique id for the trade
    :return av_price: float the average price across the filled trades minus the flat rate commission
    :return time: the time at which the order was executed
    """
    # get the order id
    order_id = order['orderId']
    # establish the entry/exit price
    ord_timestamp = order['transactTime']
    time = datetime.datetime.fromtimestamp(ord_timestamp / 1000)
    price_cumulative, comm = 0, 0
    for fill in order['fills']:
        price_cumulative += float(fill['price'])
        comm += float(fill['commission'])
        comm_asset = fill['commissionAsset']
    price_average = price_cumulative / len(order['fills'])
    usdt = float(order['cummulativeQuoteQty'])
    eth = float(order['executedQty'])

    return order_id, time, eth, usdt, comm, comm_asset, price_average

