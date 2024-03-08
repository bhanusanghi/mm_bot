import price_feeds

def getKey(d,v):
  for key,item in d.items():
    if item == v:
      return key
    
def getSymbolFromName(market):
  return market.split('-')[0]

def get_mid_price():
  return price_feeds.mid_price

def get_hubble_prices():
  return price_feeds.hubble_prices

async def callback(response):
  # print(f"Received response: {response}")
  return response
