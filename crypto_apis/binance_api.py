import requests , math
import json
"""Binance sybols:
BTCUSDT, ETHUSDT, SOLUSDT...
"""

def get_binance_server_time()->int | None:
        response = requests.get("https://api.binance.com/api/v3/time")
        if response.status_code == 200:
             json_result:dict = response.json()
             return json_result.get("serverTime",None)
        else:
            return None

def op_is_sell(transaction:dict)->bool | None: 
     """
     #Sell Operation ("m" is true): The trade represents selling BTC for USDT.   
     #Buy Operation ("m" is false): The trade represents buying BTC with USDT.
     """                                         
     opr_agr = transaction.get("m",None)
     if opr_agr == None:
           return None
     return transaction["m"] == True

def min_to_ms(min:int | float)->int:
     return int(min*60*1000)

def get_binance_trading_volume( time_window_min: float | int ,end_unix_time:int = 0 ,crypto_token:str = "BTCUSDT")->dict[str,float|int]:
   
    MAX_REQUEST_TIMEFRAME = 60000 #90k Ms is the limit time (based on some test) where you can get all aggregata trading without hitting the 1000 results limit on the binance API
    time_window_ms = min_to_ms(time_window_min)

    requests_needed:int = math.ceil(time_window_ms/MAX_REQUEST_TIMEFRAME)
    print(requests_needed)

    if end_unix_time == 0:
        end_time: int | None = get_binance_server_time()
        if end_time ==  None:
            raise Exception("Initial time for the binance server wasnt able to be established")
    else:
        end_time = end_unix_time
    
    total_transactions:int = 0
    requests_hitting_limit:int = 0 #number of requests hitting the 1000 responses API limit

    total_sell_coins: float = 0.0
    total_sell_usd:float  = 0.0
    total_buy_coins:float  = 0.0
    total_buy_usd:float  = 0.0
    
    for i in range(requests_needed):
        
        params:dict = {
            "symbol": crypto_token,
            "startTime" : end_time - MAX_REQUEST_TIMEFRAME,  #em ms , a maior janela que chega perto do limite de 1000 respostas é 90000 ms
            "endTime":    end_time,
            "limit" : 1000
        }
        #print(end_time)

        response= requests.get("https://api.binance.com/api/v3/aggTrades", params=params)
        if response.status_code == 200:
                json_result:list[dict] = response.json()
        else:
                raise Exception(F"Failure in getting aggregate trading data: {response.status_code}, response: {response.text}")
        
        num_transactions:int = len(json_result)
        total_transactions += num_transactions
    
        if num_transactions == 1000:
              requests_hitting_limit+=1
              
        for transaction in json_result:
                price:float = float(transaction.get("p",0.0))
                quantity: float = float(transaction.get("q",0.0))
                
                if op_is_sell(transaction):
                    total_sell_coins += quantity
                    total_sell_usd += quantity * price 
                else:
                    total_buy_coins += quantity
                    total_buy_usd += quantity * price 

        end_time -= MAX_REQUEST_TIMEFRAME

    print(f"requests hitting limit: {requests_hitting_limit}")
    
    if requests_hitting_limit/requests_needed > 0.5:
          raise Warning("More than half of requests are hitting the binance API rate limit")
    
    return {
        
            f"{crypto_token}_COINS_SOLD": total_sell_coins,
            f"{crypto_token}_USD_SOLD": total_sell_usd,
            f"{crypto_token}_COINS_BOUGHT": total_buy_coins,
            f"{crypto_token}_USD_BOUGHT": total_buy_usd,
            f"{crypto_token}_NET_FLOW": total_buy_usd - total_sell_usd,
            f"{crypto_token}_TOTAL_TRANSACTIONS": total_transactions
    }
   
if __name__ == "main":
   print(get_binance_trading_volume(30, crypto_token="SOLUSDT"))

#print(get_binance_server_time())
#print(json_result)
#print(len(json_result))