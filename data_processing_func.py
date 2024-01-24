from crypto_apis.binance_api import binance_trading_volume , binance_server_time, min_to_ms
import math , os
from datetime import datetime , timedelta
import pandas as pd
from dotenv import load_dotenv
load_dotenv(os.path.join("dataset_parameters.env"))

"""

On avg 80 Bytes per row of the dataframe

with 100mb we could have 970903 rows, each row being 10mins, giving us 6742 days of data or 18 years

If the window was lowered t0 5 min you could have 9 years of data with 100mb per CSV file 

If the window is 2.5min or 150s you could have 4.5 years of data or 5.5 years with 120mb (best option)


"""

MAX_TIME_FRAME_HOURS = int(os.getenv("MAX_TIME_FRAME_HOURS")) # type: ignore /// how far back will data be collected in hours, equivalent to 5,5 years 

CRYPTO_TOKEN_NAMES = {"BTCUSDT", "ETHUSDT", "SOLUSDT"} #Binance names for the accepted crypto tokens and their value in USDT 


def add_crypto_dataframes(newer_data:pd.DataFrame , older_data: pd.DataFrame)->pd.DataFrame:
    """
    The lower the index the more recent the data is

    Dates on the newer data df need to be more recent than on the older data

    No data is supposed to be lost on this function
    
    """
    if not isinstance(newer_data, pd.DataFrame) or not isinstance( older_data, pd.DataFrame):
        raise TypeError("input data isnt a pandas Dataframe")

    newer_date = newer_data.at[0,"DATE"]
    old_date = older_data.at[0,"DATE"]
    print(newer_date,old_date)
    if old_date > newer_date:
        raise IOError("most recent date from the older dataframe is more recent than the dates from the newer dataframe")

    return pd.concat(objs=[newer_data,older_data], axis= 0, ignore_index=True)

def update_crypto_dataframes(newer_data:pd.DataFrame , older_data: pd.DataFrame, max_row_num: int = 1000000)->pd.DataFrame:
    """
    This function will keep the max size of the current older_data df and remove its older indexes 
    while adding newer data to its top
    
    """
    if not isinstance(newer_data, pd.DataFrame) or not isinstance( older_data, pd.DataFrame):
        raise TypeError("input data isnt a pandas Dataframe")


    older_data_row_num:int = older_data.shape[0]  
    newer_data_row_num:int = newer_data.shape[0]

    if older_data_row_num + newer_data_row_num <= max_row_num:
        raise IOError("The new size after apending the dataframes doesnt cross the limit, please use the `add_crypto_dataframes` function instead  ")
    else:
        rows_to_remove: int = (older_data_row_num + newer_data_row_num) - max_row_num
    
    first_row_to_remove:int = older_data_row_num - rows_to_remove
    older_data = older_data.drop(index=[i for i in range(first_row_to_remove, older_data_row_num)])

    return pd.concat(objs=[newer_data,older_data], axis= 0, ignore_index=True)

def crypto_data_to_df(time_frame_hours:int|float,crypto_token: str, end_unix_time:int = 0)-> pd.DataFrame: 
    """
    TODO
    make this function not throw exceptions but rather return None on errors and handle theses Nones on the main.py functions, to retry gathering data on a certain interval   
    """
    if not isinstance(time_frame_hours,int) and not isinstance(time_frame_hours,float):
        raise TypeError("Input time frame isnt an int or float")
    if time_frame_hours > MAX_TIME_FRAME_HOURS:
        raise IOError(f"Amount of hours exceeds MAX_TIME_FRAME of {MAX_TIME_FRAME_HOURS} hours ({MAX_TIME_FRAME_HOURS/24/365} years)")
    if crypto_token not in CRYPTO_TOKEN_NAMES:
        raise IOError(f"crypto token not supported, heres the list of supported ones {CRYPTO_TOKEN_NAMES}")
    
    time_frame_mins:int | float = time_frame_hours * 60
    DATA_TIME_WINDOW_MIN = int(os.getenv("DATA_TIME_WINDOW_MIN")) # type: ignore /// how many minutes of data does each column represent
    
    num_rows:int = math.ceil(time_frame_mins/DATA_TIME_WINDOW_MIN) #how many time_window rows will be needed to cover the entire time_frame passed as arg

    columns_list:list[str] = [   
                  "DATE", 
                 f"{crypto_token}_INITIAL_PRICE",
                 f"{crypto_token}_FINAL_PRICE",
                 f"{crypto_token}_COINS_BOUGHT",
                 f"{crypto_token}_COINS_SOLD",
                 f"{crypto_token}_NET_FLOW",
                 f"{crypto_token}_TOTAL_AGGRT_TRANSACTIONS"
                            ]
    
    df = pd.DataFrame(columns = columns_list)
    
    if end_unix_time == 0:
        start_date = datetime.now().replace(microsecond=0)
        cur_unix_time: int | None = binance_server_time()

        if cur_unix_time == None:
         raise Exception("Initial time for the binance server wasnt able to be established")
    else:
         start_date = datetime.fromtimestamp(end_unix_time)
         cur_unix_time = end_unix_time

    for i in range(num_rows): #time window loop, each iteration adds a row to the df

        cur = crypto_token + "USDT" #binance symbol for token to USDT (TETHER) price   
        data:dict | None = binance_trading_volume(
                    time_window_min=DATA_TIME_WINDOW_MIN,
                    end_unix_time= cur_unix_time,
                    crypto_token= cur
                   )
        if data == None: #in case the row data is incomplete, just skip that time frame 
            continue
        currency_data:list[float|int|datetime] = [
                        start_date, 
                        data.get(f"{cur}_INITIAL_PRICE",None),
                        data.get(f"{cur}_FINAL_PRICE",None),
                        data.get(f"{cur}_COINS_BOUGHT",None),
                        data.get(f"{cur}_COINS_SOLD",None),
                        data.get(f"{cur}_NET_FLOW",None),
                        data.get(f"{cur}_TOTAL_AGGRT_TRANSACTIONS",None)
                    ]
        
        cur_unix_time  -= min_to_ms(DATA_TIME_WINDOW_MIN)
        df.loc[i] = currency_data # type: ignore
        start_date -= timedelta(minutes=DATA_TIME_WINDOW_MIN)
    
    return df


#df: pd.DataFrame = crypto_data_to_df(0.5, "BTC" )
#df.to_csv("teste2.csv", index= False)