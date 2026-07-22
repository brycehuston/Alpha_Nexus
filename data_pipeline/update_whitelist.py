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
    results = dune.get_latest_result(dune_query_id)
    
    # Filter and sort the wallets based on Option A requirements
    elite_wallets = []
    for row in results.get_rows():
        if 'wallet_address' not in row:
            continue
            
        # Parse metrics with fallbacks in case the query isn't fully updated yet
        win_rate = float(row.get('win_rate', 0.0))
        total_trades = int(row.get('total_trades', 0))
        net_profit_sol = float(row.get('net_profit_sol', 0.0))
        
        # Apply strict filtering criteria
        if win_rate > 0.65 and total_trades > 50 and net_profit_sol > 10.0:
            elite_wallets.append({
                'address': row['wallet_address'],
                'net_profit_sol': net_profit_sol
            })

    if not elite_wallets:
        print("WARNING: Dune returned 0 wallets passing criteria. Skipping Redis update to avoid wiping whitelist.")
        return

    # Sort by net_profit_sol descending and slice top 50
    elite_wallets.sort(key=lambda x: x['net_profit_sol'], reverse=True)
    top_50 = elite_wallets[:50]
    wallets = list(set([w['address'] for w in top_50]))

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
