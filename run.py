import os
import requests

API_KEY = os.getenv("TWELVE_API_KEY")

if not API_KEY:
    raise RuntimeError("Brak TWELVE_API_KEY w env. Dodaj go w Settings → Secrets and variables → Actions.")

url = "https://api.twelvedata.com/time_series"
params = {
    "symbol": "EUR/USD",
    "interval": "15min",
    "outputsize": 1,
    "timezone": "Europe/Warsaw",
    "apikey": API_KEY
}

r = requests.get(url, params=params, timeout=20)
data = r.json()

# Jeśli API zwróciło błąd, pokaż go w logach
if data.get("status") != "ok" or "values" not in data:
    print("RAW RESPONSE FROM TWELVE DATA:")
    print(data)
    raise RuntimeError("Brak 'values' w odpowiedzi. Patrz RAW RESPONSE powyżej.")

candle = data["values"][0]
print("EUR/USD M15")
print("Time:", candle["datetime"])
print("O H L C:", candle["open"], candle["high"], candle["low"], candle["close"])
