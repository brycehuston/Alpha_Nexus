import os
import redis
from dune_client.client import DuneClient
from dune_client.query import QueryBase

def update_redis_whitelist():
    dune_api_key = os.environ.get("DUNE_API_KEY")
    dune_query_id = int(os.environ.get("DUNE_QUERY_ID", 0))

    if not dune_api_key:
        print("ERROR: DUNE_API_KEY not set. Aborting whitelist update.")
        return
    if not dune_query_id:
        print("ERROR: DUNE_QUERY_ID not set. Aborting whitelist update.")
        return

    print(f"Fetching smart wallets from Dune query {dune_query_id}...")
    dune = DuneClient(api_key=dune_api_key)
    # We fetch the latest results instead of forcing an execution because free tier
    # or non-owner API keys throw a 403 Forbidden on the /execute endpoint.
    results = dune.get_latest_query_results(dune_query_id)
    wallets = [row['smart_wallet_address'] for row in results.get_rows()]

    if not wallets:
        print("WARNING: Dune returned 0 wallets. Skipping Redis update to avoid wiping whitelist.")
        return

    print(f"Updating Redis with {len(wallets)} elite wallets...")
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    pipe = r.pipeline()
    pipe.delete("smart_herd_wallets")
    pipe.sadd("smart_herd_wallets", *wallets)
    pipe.execute()
    print(f"Redis updated. Restarting alphanexus-daemon...")
    os.system("sudo systemctl restart alphanexus-daemon")
    print("Done.")

if __name__ == "__main__":
    update_redis_whitelist()
