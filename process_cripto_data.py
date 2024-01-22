from crypto_apis.binance_api import get_binance_trading_volume , get_binance_server_time, min_to_ms
import requests , math
from datetime import datetime , timedelta
import pandas as pd
"""





"""



def crypto_data_to_df(time_frame_hours:int | float, start_time_unix:int = 0)-> pd.DataFrame: 
    time_frame_hours = time_frame_hours * 60
    CRYPTO_CURRUNCIES = ["BTC"] #,"ETH","SOL"
    DATA_TIME_WINDOW_MIN = 30 #how many minutes of data does each column represent
    num_columns = math.ceil(time_frame_hours/DATA_TIME_WINDOW_MIN)

    columns_list:list[str] = []
    for cur in CRYPTO_CURRUNCIES:
        columns_list.extend(
             [
                 f"{cur}_COINS_BOUGHT",
                 f"{cur}_COINS_SOLD",
                 f"{cur}_NET_FLOW",
                 f"{cur}_TOTAL_TRANSACTIONS"
             ]
        )
    df = pd.DataFrame(columns = columns_list)
    print(df)
    #df = df.set_axis(axis=1, labels=columns_list)
    
    start_date = datetime.now()
    cur_unix_time: int | None = get_binance_server_time()

    if cur_unix_time == None:
       raise Exception("Initial time for the binance server wasnt able to be established")

    for i in range(num_columns): #time window loop
        print(i)

        currency_data:list[float|int] = []
        for cur in CRYPTO_CURRUNCIES: #currency loop 
            cur += "USDT"
            
            data:dict = get_binance_trading_volume(
                    time_window_min=DATA_TIME_WINDOW_MIN,
                    end_unix_time= cur_unix_time,
                    crypto_token= cur
            )
            currency_data.extend(
                [
                data.get(f"{cur}_COINS_BOUGHT",-1),
                data.get(f"{cur}_COINS_SOLD",-1),
                data.get(f"{cur}_NET_FLOW",-1),
                data.get(f"{cur}_TOTAL_TRANSACTIONS",-1)
                ]
            )
            
            
        cur_unix_time  -= min_to_ms(DATA_TIME_WINDOW_MIN)
        df.loc[start_date] = currency_data # type: ignore
        start_date += timedelta(minutes=DATA_TIME_WINDOW_MIN)
    
    
    print(df)

    return df

   # get_binance_trading_volume()
    #pass

crypto_data_to_df(1)