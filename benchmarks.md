## Benchmark 1

# Binance API:

* 4 apis requests per row, each one ranging from 0.4 - 1.2 seconds (avg of 0.6-0.8)

# DF row 

* 2,11 seconds is taken by the binance APIs, but the total time is around 3.1 seconds, so theres 1 second of further delay


# Entire DF


# main func

* very little difference 0.01 seconds between each chunk loop and the time waiting for the create_crypto_dataframe func