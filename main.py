from crypto_data_etl import CryptoDataETL , setup_logging
import pandas as pd
import time , sys, logging
from datetime import datetime

setup_logging()
main_logger = logging.getLogger("crypto_data_etl_main")
APP_STATES:list[str] = ["START", "UPDATE"]

def time_lag_hours(df:pd.DataFrame)->float:
    if not isinstance(df, pd.DataFrame):
            raise TypeError("in function set_unix_time_from_df: Input param df isnt of type Pandas Dataframe")
    if df.shape[0] == 0:
            raise TypeError("in function set_unix_time_from_df: Input dataframe is empty")
            
    newest_date: datetime = df.at[0, "DATE"]
    latest_unix_time:int = (int(datetime.timestamp(newest_date)) * 1000)
    cur_unix_time:int = int(time.time() * 1000)

    dif:int = cur_unix_time - latest_unix_time

    return dif/3600000

def setup_argv()->tuple[str,str]:
    num_args:int  = len(sys.argv)
    if num_args < 3:
        main_logger.exception("Not enough command-line arguments were passed")
        raise Exception("Not enough command-line arguments were passed")

    state_arg:str = sys.argv[1]
    state_arg = state_arg.upper()

    if state_arg not in APP_STATES:
        main_logger.exception("command-line arguments are not START or UPDATE")
        raise Exception("command-line arguments are not START or UPDATE")
    crypto_token_arg: str = sys.argv[2].upper()

    return state_arg, crypto_token_arg

def save_df(df: pd.DataFrame):
    df.to_csv("teste4.csv",index=False)
    pass

def change_state_lambda(new_state, time_lag_hours: float):
    pass

def update_sucess_lambda()->bool:
    return True

def error_warning_lambda(error_message:str)->bool:
    return True

def get_df()->pd.DataFrame | None:
    return None

state_arg, crypto_token_arg = setup_argv()
etl: CryptoDataETL = CryptoDataETL(crypto_token=crypto_token_arg)
df: pd.DataFrame | None

if state_arg == "START":
   if get_df():
       msg = f"Dataset of the crypto token {crypto_token_arg} already exists, consider using the UPDATE STATE"
       main_logger.exception(msg)
       error_warning_lambda(msg)
       raise Exception(msg)
   try:
       print("trying to get dataset")
       df = etl.create_dataset()
       print("got dataset")
   except Exception as e:
       main_logger.exception(f"During start of the dataset collection, it wasnt possible to extract the initial dataset, error {e}")
       raise Exception(f"During start of the dataset collection, it wasnt possible to extract the initial dataset, error {e}")
   
   save_df(df)
   time_lag: float = time_lag_hours(df)
   print(f"time lag {time_lag}")
   max_tries: int = 7
 
   if time_lag > 12.0: #loop to try to make the dataset cover dates at max 12h ago
        for i in range(max_tries):
           df = etl.update_dataset(df)
           time_lag = time_lag_hours(df)
           save_df(df)
           if time_lag <= 12.0:
               break
        else:
           error_msg = f"After initial dataset was created, it was tried {max_tries} times to update it to current date, but it was unsucessful"
           main_logger.exception(error_msg)
           error_warning_lambda(error_msg)
           raise Exception(error_msg)
   
   change_state_lambda("UPDATE", time_lag)     

elif state_arg == "UPDATE": #in this case we are going to update the data within a certain timewindow (12h or less)
    df = get_df()

    if df is None:
        msg = "During the UPDATE state it wasnt possible to get the dataset"
        main_logger.exception(msg)
        raise Exception(msg)
    try:
       df = etl.update_dataset(df)
    except:
        msg = "During the UPDATE state it wasnt possible to update the dataset with new data"
        main_logger.exception(msg)
        raise Exception(msg)
    
    save_df(df)
    update_sucess_lambda()






"""
class AppState(Enum):
    START = "START",
    UPDATE = "UPDATE"

class DataApplication():
    crypto_token: str
    state: AppState
    data_etl: CryptoDataETL

    def __init__(self, crypto_token:str, state: AppState)->None:
        self.crypto_token = crypto_token
        self.state = state

    def run_crypto_etl(self,crypto_token:str)->None:
        ETL: CryptoDataETL = CryptoDataETL(crypto_token=crypto_token)
        df:pd.DataFrame = ETL.create_dataset()

    def time_lag_hours(self, df:pd.DataFrame)->float:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("in function set_unix_time_from_df: Input param df isnt of type Pandas Dataframe")
        if df.shape[0] == 0:
            raise TypeError("in function set_unix_time_from_df: Input dataframe is empty")
            
        newest_date: datetime = df.at[0, "DATE"]
        latest_unix_time:int = (int(datetime.timestamp(newest_date)) * 1000)
        cur_unix_time:int = int(time.time() * 1000)

        dif:int = cur_unix_time - latest_unix_time

        return dif/3600000

    def start_etl(self)->None:
        pass

    def update_cycle(self)->None:
        pass"""