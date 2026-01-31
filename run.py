import os
import requests

API_KEY = os.getenv("TWELVE_API_KEY")

url = "https://api.twelvedata.com/time_series"
params = {
    "symbol": "EUR/USD",
    "interval": "15min",
    "outputsize": 1,
    "timezone": "Europe/Warsaw",
    "apikey": API_KEY
}

r = requests.get(url, params=params)
data = r.json()

candle = data["values"][0]
print("EUR/USD M15")
print("Time:", candle["datetime"])
print("O H L C:", candle["open"], candle["high"], candle["low"], candle["close"])

