import os, redis
from dune_client.client import DuneClient
from dune_client.query import QueryBase

def update_redis_whitelist():
    dune = DuneClient(api_key=os.environ.get("DUNE_API_KEY"))
    query = QueryBase(name="Smart Herd Sync", query_id=1234567) # Update with real ID
    results = dune.run_query(query)
    wallets = [row['smart_wallet_address'] for row in results.get_rows()]
    
    if wallets:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        pipe = r.pipeline()
        pipe.delete("smart_herd_wallets")
        pipe.sadd("smart_herd_wallets", *wallets)
        pipe.execute()
        os.system("sudo systemctl restart alphanexus-daemon")

if __name__ == "__main__":
    update_redis_whitelist()
