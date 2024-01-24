from data_processing_func import update_crypto_dataframes, add_crypto_dataframes, crypto_data_to_df
import logging.config , json , os , math
from dotenv import load_dotenv
load_dotenv(os.path.join("dataset_parameters.env"))
"""
TODO
See if it makes sense to join the main etl functions in a class, considering theres state that influnces both of their actions 
(filling or updating dataset, num of rows in the file..)
"""

logger = logging.getLogger("crypto_data_etl")
log_config:dict

with open(os.path.join("logger_config.json"), "r") as f:
    log_config = json.load(f)

def fill_dataset(crypto_token:str)->None:
   """
   Fills the dataset with 5,5 years of binance data for that crypto_token
   """
   MAX_TIME_FRAME_HOURS:int = int(os.getenv("MAX_TIME_FRAME_HOURS ")) # type: ignore
   DATA_TIME_WINDOW_MIN:int = int(os.getenv("DATA_TIME_WINDOW_MIN")) # type: ignore

   max_num_rows:int = math.ceil( (MAX_TIME_FRAME_HOURS * 60) / DATA_TIME_WINDOW_MIN)

   chunks_of_data:int = 20 #in how many data chunks we are going to split the extraction 
   
   rows_per_chunk:int  =  math.ceil(max_num_rows/chunks_of_data)

   unix_time_per_chunk:int = rows_per_chunk * DATA_TIME_WINDOW_MIN * 60 *1000 #how many ms (unix time) were covered by the chunk of data
   pass

def update_dataset()->None:
   pass 
