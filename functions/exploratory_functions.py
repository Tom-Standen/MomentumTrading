"""
Functions to perform exploratory analysis on momentum trading strategies
"""

import pandas as pd
import numpy as np


def frama_perf(InputPrice, batch):
    """
        frama with numpy for many datapoints
        InputPrice: The input time-series to estimate its frama per batch
        batch: The batch of datapoints, where the N1 and N2 are calculated
        See also: http://www.stockspotter.com/Files/frama.pdf 
    """
    Length = len(InputPrice)
    # calucate maximums and minimums
    H = np.array([np.max(InputPrice[i:i + batch]) for i in range(0, Length - batch, 1)])
    L = np.array([np.min(InputPrice[i:i + batch]) for i in range(0, Length - batch, 1)])

    # set the N-variables
    b_inv = 1.0 / batch
    N12 = (H - L) * b_inv
    N1 = N12[0:-1]
    N2 = N12[1:]
    N3 = (np.array([np.max(H[i:i + 1]) for i in range(0, len(H) - 1)]) - np.array(
        [np.min(L[i:i + 1]) for i in range(0, len(H) - 1)])) / batch

    # calculate the fractal dimensions
    Dimen = np.zeros(N1.shape)
    Dimen_indices = np.bitwise_and(np.bitwise_and((N1 > 0), (N2 > 0)), (N3 > 0))
    lg2_inv = 1.0 / np.log2(2)
    Dimen[Dimen_indices] = (np.log2(N1 + N2) - np.log2(N3)) * lg2_inv

    # calculate the filter factor
    alpha = np.exp(-4.6 * (Dimen) - 1)
    alpha[alpha < 0.1] = 0.1
    alpha[alpha > 1] = 1

    Filt = np.array(InputPrice[0:len(alpha)])
    # Declare two variables to accelerate performance
    S = 1 - alpha
    A = alpha * InputPrice[0:len(alpha)]
    # This is the overheat of all the computation
    # where the filter applies to the input
    for i in range(0, Length - 2 * batch, 1):
        Filt[i + 1] = A[i] + S[i] * Filt[i]

    # add to the function so that it produces a numpy array of the right shape to fit into the dataframe
    Filt = np.pad(Filt, (batch + 1, 0), mode='constant', constant_values=(np.nan,))
    return Filt


def golden_death_cross_ma_test(candle_df, mode, fast_avs=[5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
                               slow_avs=[1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5, 5.5, 6], transaction_cost=0.99):
    '''
    Test which combo of simple averages produce the best returns
    
    TRADING STRATEGY 6 - GOLDEN and DEATH CROSS with fractal moving averages

    Less well known but literature holds in high esteem, similar to exp but use fractal dimension instead of deacy - 
    
    Originial Paper - Ehlers, John. "FRAMA–Fractal Adaptive Moving Average." Technical Analysis of Stocks & Commodities (2005).
    
    GitHub with high performance frama function: https://github.com/supernlogn/frama
    

    Strategy: Go Long at the Golden Cross and Go short at the Death Cross
    
    args:
    candle_df: dataframe - candlestick dataframe with a 'close' column like from OANDA/Binance API
    mode: str - must be one of ['simple', 'exponential', 'frama'] decide what type of moving average to use
    fast_avs (fc): list of fast moving/short-term averages to test against
    slow_avs (sc): list of multipliers for fast moving - each multiplier is tested against every fast average

    '''
    gdc_results = pd.DataFrame(columns=['fc', 'sc', 'real_return_max', 'real_return_av', 'real_return_close'])
    df_index = 0

    for fc in fast_avs:
        print(fc)
        for sc in slow_avs:
            # long and short av used for naming convention
            long = round(fc * sc)
            short = fc
            if long > len(candle_df):
                # if the length of the moving average is greater than number of rows in dataframe (most likely for
                # daily) enter 0 values and pass over
                gdc_results.loc[df_index] = [short, sc, 0, 0, 0]
                df_index += 1
            else:
                if mode == 'simple':
                    # simply average the closing prices in the defined period
                    # need to include shift() to not include todays price
                    candle_df['ma_' + str(short)] = candle_df['close'].rolling(short).mean().shift(1)
                    candle_df['ma_' + str(long)] = candle_df['close'].rolling(long).mean().shift(1)
                elif mode == 'exponential':
                    # use pandas to calculate exponential moving average
                    candle_df['ma_' + str(short)] = candle_df['close'].ewm(span=fc).mean()
                    candle_df['ma_' + str(long)] = candle_df['close'].ewm(span=long).mean()
                elif mode == 'frama':
                    # use GitHub function frama_perf to calculate frama
                    candle_df['ma_' + str(short)] = frama_perf(candle_df['close'], fc)
                    candle_df['ma_' + str(long)] = frama_perf(candle_df['close'], long)
                else:
                    print('You did not pick a valid moving average')

                # string for column naming
                short_long = str(short) + '_' + str(long)
                # calculate whether we are long or short - long if rolling_15 > rolling_100 and vice versa
                candle_df['position_' + str(short_long)] = np.sign(
                    candle_df['ma_' + str(short)] - candle_df['ma_' + str(long)])
                # trade when there's a change of sign
                candle_df['trade_' + str(short_long)] = (candle_df['position_' + str(short_long)].dropna() != candle_df[
                    'position_' + str(short_long)].dropna().shift(1))

                temp_df = candle_df[candle_df['trade_' + str(short_long)].fillna(False)]

                temp_df['log_returns_tr'] = np.log((temp_df['close'] / temp_df['close'].shift(1)))
                temp_df['strategy_' + str(short_long)] = temp_df['log_returns_tr'] * temp_df[
                    'position_' + str(short_long)].shift(1)
                temp_df['return_' + str(short_long)] = temp_df['strategy_' + str(short_long)].dropna().cumsum().apply(
                    np.exp)

                # real returns
                temp_df['real_strategy_' + str(short_long)] = temp_df['strategy_' + str(short_long)] + np.log(
                    transaction_cost)
                temp_df['real_return_' + str(short_long)] = temp_df[
                    'real_strategy_' + str(short_long)].dropna().cumsum().apply(np.exp)

                # return metrics
                real_return_max = temp_df['real_return_' + str(short_long)].max()
                real_return_av = temp_df['real_return_' + str(short_long)].mean()
                real_return_close = temp_df['real_return_' + str(short_long)][temp_df.index[-1]]

                gdc_results.loc[df_index] = [short, sc, real_return_max, real_return_av, real_return_close]
                df_index += 1
    print('done')
    gdc_results = gdc_results.sort_values(by='real_return_av', ascending=False)
    return gdc_results

