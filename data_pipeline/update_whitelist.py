import os
import redis
import requests

def update_redis_whitelist():
    dune_api_key = os.environ.get("DUNE_API_KEY")
    dune_query_id = os.environ.get("DUNE_QUERY_ID", "0")

    if not dune_api_key:
        print("ERROR: DUNE_API_KEY not set. Aborting whitelist update.")
        return
    if not dune_query_id or dune_query_id == "0":
        print("ERROR: DUNE_QUERY_ID not set. Aborting whitelist update.")
        return

    print(f"Fetching smart wallets from Dune query {dune_query_id}...")

    # Use the free-tier compatible /query/{id}/results endpoint directly.
    # The dune_client library's get_latest_result() routes through the execution
    # endpoint which requires a paid plan (402). This endpoint works on free tier.
    url = f"https://api.dune.com/api/v1/query/{dune_query_id}/results"
    headers = {"X-Dune-API-Key": dune_api_key}
    params = {"limit": 1000}

    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        print(f"ERROR: Dune API returned {resp.status_code}: {resp.text}")
        return

    rows = resp.json().get("result", {}).get("rows", [])
    print(f"Dune returned {len(rows)} raw rows.")

    # Filter and sort the wallets based on criteria
    elite_wallets = []
    for row in rows:
        if 'wallet_address' not in row:
            continue

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
        print("WARNING: 0 wallets passed the filter criteria. Skipping Redis update.")
        print("(Tip: Check your Dune query column names match: wallet_address, win_rate, total_trades, net_profit_sol)")
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
    print(f"✅ Redis updated with {len(wallets)} elite wallets. Ready to run the bot!")

if __name__ == "__main__":
    update_redis_whitelist()
