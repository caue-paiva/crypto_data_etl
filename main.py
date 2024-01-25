from data_processing_func import update_crypto_dataframes, add_crypto_dataframes, create_crypto_dataframe
from crypto_apis.binance_api import binance_server_time
import logging.config , json , os , math , time
from dotenv import load_dotenv
from typing import Callable
import pandas as pd
load_dotenv(os.path.join("dataset_parameters.env"))

"""
TODO
1)See if it makes sense to join the main etl functions in a class, considering theres state that influnces both of their actions 
(filling or updating dataset, num of rows in the file..)

2) make a speed and exec time profile of my code to see what is making it slower and try to optimize it (see if the .env variables are making them slower)

3) Add error handling and retries to the save_file_function
"""

logger = logging.getLogger("crypto_data_etl")
log_config:dict

with open(os.path.join("logger_config.json"), "r") as f:
    log_config = json.load(f)

logging.config.dictConfig(config=log_config)

def write_df_file(df:pd.DataFrame)->None:
   df.to_csv("teste3.csv", index=False)

def fill_dataset(crypto_token:str, save_file_func: Callable[[pd.DataFrame],None])-> None:
   """
   Fills the empty CSV dataset with 5,5 years of binance data for a certain crypto token

   Args:
         crypto_token (str) : crypto asset that the data will belong to, needs to be on binance token format (ex. BTCUSDT)
   
         save_file_func (Callable[pd.Dataframe] -> None) : function to save the pandas dataframe in some way,
         either saving locally or writing on some cloud storage
   """
   MAX_TIME_FRAME_HOURS:int = 4 #int(os.getenv("MAX_TIME_FRAME_HOURS ")) # type: ignore
   DATA_TIME_WINDOW_MIN:int = int(os.getenv("DATA_TIME_WINDOW_MIN")) # type: ignore

   max_num_rows:int = math.ceil( (MAX_TIME_FRAME_HOURS * 60) / DATA_TIME_WINDOW_MIN)

   CHUNKS_OF_DATA:int = 2 #in how many data chunks we are going to split the extraction 
   hours_per_chunk:int = math.ceil(MAX_TIME_FRAME_HOURS/CHUNKS_OF_DATA)
   rows_per_chunk:int  =  math.ceil(max_num_rows/CHUNKS_OF_DATA)

   unix_time_per_chunk:int = rows_per_chunk * DATA_TIME_WINDOW_MIN * 60 *1000 #how many ms (unix time) were covered by the chunk of data
   
   extracted_chunks:int = 0
   
   cur_unix_time = binance_server_time()

   time_tries:int  = 0
   max_time_tries: int = 1000
   while cur_unix_time == None:
       cur_unix_time = binance_server_time()
       if time_tries >= max_time_tries:
          logger.exception(f"failed to get binance unix server time after {max_time_tries} tries")
          raise Exception(f"failed to get binance unix server time after {max_time_tries} tries")
       print("binance_server_time_failed")
       time_tries+= 1

   df: pd.DataFrame = pd.DataFrame()
   first_data_chunk:bool = True
   
   chunk_tries:int = 0
   max_chunk_tries:int = 20
   while extracted_chunks < CHUNKS_OF_DATA:
      chunk_start: float = time.time()
      if chunk_tries > max_chunk_tries:
          logger.exception(f"tried to extract the chunk number {extracted_chunks+1}, {max_chunk_tries} times but none were sucessful, shutting down programn")
          raise Exception(f"tried to extract the chunk number {extracted_chunks+1}, {max_chunk_tries} times but none were sucessful, shutting down programn")
      
      try:
            crypto_df_time:float = time.time()
            chunk_df: pd.DataFrame | None = create_crypto_dataframe(
                    time_frame_hours=hours_per_chunk,
                    crypto_token=crypto_token,
                    end_unix_time = cur_unix_time # type: ignore
            )
            print(f"it took the time {time.time()- crypto_df_time} to get a crypto DF")
            if not isinstance(chunk_df,pd.DataFrame):
                raise ValueError("data-chunk returned is none")
            
            if first_data_chunk:
                df = chunk_df
                first_data_chunk = False
            else:
                df = add_crypto_dataframes(newer_data=df, older_data=chunk_df)
            
            extracted_chunks += 1
            cur_unix_time -= unix_time_per_chunk # type: ignore
            chunk_tries = 0
            print(f"sucessful chunk , it took the following time: {time.time() -  chunk_start }")

      except Exception as e:
            logger.info(f"tried to extract the chunk number {extracted_chunks+1}, csv currently has {extracted_chunks* rows_per_chunk} lines ,exception was {e}")
            logger.exception("failed to get a data chunk, re-trying")
            chunk_tries+=1
            time.sleep(0.5)
   
   save_file_tries:int = 0
   max_save_file_tries:int = 20
   while save_file_tries < max_chunk_tries:
     try:
        save_file_func(df)
        break
     except Exception as e:
        logger.info(f"Tried to save the data with the save_file_function ,exception was {e}")
        logger.exception("failed to get a data chunk, re-trying")
        save_file_tries += 1
        time.sleep(0.5)
   else:
       logger.exception(f"Tried to save the data with the save_file_function ,wasnt able to do it after {max_save_file_tries} tries")
       
      
def update_dataset()->None:
   pass 


fill_dataset("BTCUSDT", write_df_file)
