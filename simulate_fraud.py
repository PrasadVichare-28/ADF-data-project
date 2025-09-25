
#!/usr/bin/env python3
import argparse, math, random
from datetime import datetime, timedelta
import numpy as np, pandas as pd

EARTH_R = 6371.0

def haversine_km(lat1, lon1, lat2, lon2):
    import math
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * EARTH_R * math.asin(math.sqrt(a))

def jitter_deg(km): return km / 111.0

def gen_coordinates(n, center_lat, center_lon, radius_km, rng):
    pts = []
    for _ in range(n):
        ang = rng.uniform(0, 2*math.pi)
        r = radius_km * (rng.random()**0.5)
        lat = center_lat + jitter_deg(r) * math.cos(ang)
        lon = center_lon + jitter_deg(r) * math.sin(ang)
        pts.append((lat, lon))
    return pts

def simulate(args):
    rng = random.Random(args.seed)
    np_rng = np.random.default_rng(args.seed)
    center_lat, center_lon = 41.8781, -87.6298  # Chicago

    customers = gen_coordinates(args.customers, center_lat, center_lon, 50, rng)
    terminals = gen_coordinates(args.terminals, center_lat, center_lon, 70, rng)

    cust_df = pd.DataFrame(customers, columns=["CUSTOMER_LAT","CUSTOMER_LON"])
    cust_df["CUSTOMER_ID"] = [f"C{str(i).zfill(7)}" for i in range(args.customers)]
    term_df = pd.DataFrame(terminals, columns=["TERMINAL_LAT","TERMINAL_LON"])
    term_df["TERMINAL_ID"] = [f"T{str(i).zfill(6)}" for i in range(args.terminals)]

    NEAR_KM = 10
    cust_near = []
    for _, crow in cust_df.iterrows():
        near = []
        for j, trow in term_df.iterrows():
            if haversine_km(crow.CUSTOMER_LAT, crow.CUSTOMER_LON, trow.TERMINAL_LAT, trow.TERMINAL_LON) <= NEAR_KM:
                near.append(j)
        if not near: near = rng.sample(range(len(term_df)), k=min(10, len(term_df)))
        cust_near.append(near)

    FRAUD_STOLEN_CARD_RATE = 0.000086
    FRAUD_BURST_TX = (2, 5)
    FRAUD_FAR_KM = 35
    LEGIT_TXN_LAMBDA = 0.15
    LEGIT_AMOUNT_LOGN = (3.4, 0.9)

    start_date = datetime.fromisoformat(args.start)
    out_dir = args.out

    import os
    os.makedirs(out_dir, exist_ok=True)
    txn_counter = 0

    for day_idx in range(args.days):
        day_date = start_date + timedelta(days=day_idx)
        rows = []

        legit_counts = np_rng.poisson(LEGIT_TXN_LAMBDA, size=args.customers)

        compromised = [i for i in range(args.customers) if rng.random() < FRAUD_STOLEN_CARD_RATE]
        far_idx = {}
        for ci in compromised:
            crow = cust_df.iloc[ci]
            far = []
            for j, trow in term_df.iterrows():
                if haversine_km(crow.CUSTOMER_LAT, crow.CUSTOMER_LON, trow.TERMINAL_LAT, trow.TERMINAL_LON) >= FRAUD_FAR_KM:
                    far.append(j)
            if not far: far = rng.sample(range(len(term_df)), k=min(20, len(term_df)))
            far_idx[ci] = far

        # legit
        for ci in range(args.customers):
            k = int(legit_counts[ci])
            if k <= 0: continue
            near = cust_near[ci]
            for _ in range(k):
                j = rng.choice(near)
                sec = rng.randint(0, 86400-1)
                dt = day_date + timedelta(seconds=sec)
                mu, sigma = LEGIT_AMOUNT_LOGN
                amount = float(np.random.lognormal(mu, sigma))
                amount = round(min(amount, 5000.0), 2)
                txn_counter += 1
                rows.append({
                    "TRANSACTION_ID": f"TX{txn_counter:012d}",
                    "TX_DATETIME": dt.isoformat(),
                    "TX_TIME_SECONDS": sec,
                    "TX_TIME_DAYS": day_idx,
                    "CUSTOMER_ID": cust_df.iloc[ci]["CUSTOMER_ID"],
                    "TERMINAL_ID": term_df.iloc[j]["TERMINAL_ID"],
                    "TX_AMOUNT": amount,
                    "TX_FRAUD": 0,
                    "TX_FRAUD_SCENARIO": ""
                })

        # fraud bursts
        for ci in compromised:
            base_sec = rng.randint(0, 86400-60)
            for _ in range(rng.randint(*FRAUD_BURST_TX)):
                j = rng.choice(far_idx[ci])
                sec = min(base_sec + rng.randint(0, 120), 86400-1)
                dt = day_date + timedelta(seconds=sec)
                amount = float(np.random.lognormal(3.8, 0.7))
                amount = round(min(max(amount, 50.0), 8000.0), 2)
                txn_counter += 1
                rows.append({
                    "TRANSACTION_ID": f"TX{txn_counter:012d}",
                    "TX_DATETIME": dt.isoformat(),
                    "TX_TIME_SECONDS": sec,
                    "TX_TIME_DAYS": day_idx,
                    "CUSTOMER_ID": cust_df.iloc[ci]["CUSTOMER_ID"],
                    "TERMINAL_ID": term_df.iloc[j]["TERMINAL_ID"],
                    "TX_AMOUNT": amount,
                    "TX_FRAUD": 1,
                    "TX_FRAUD_SCENARIO": "STOLEN_CARD_FAR_BURST"
                })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.merge(cust_df[["CUSTOMER_ID","CUSTOMER_LAT","CUSTOMER_LON"]], on="CUSTOMER_ID", how="left")
            df = df.merge(term_df[["TERMINAL_ID","TERMINAL_LAT","TERMINAL_LON"]], on="TERMINAL_ID", how="left")
            df = df.sort_values("TX_TIME_SECONDS")

        out_path = f"{out_dir}/transactions_{day_date.strftime('%Y%m%d')}.csv"
        cols = ["TRANSACTION_ID","TX_DATETIME","TX_TIME_SECONDS","TX_TIME_DAYS","CUSTOMER_ID","TERMINAL_ID",
                "TX_AMOUNT","TX_FRAUD","TX_FRAUD_SCENARIO","CUSTOMER_LAT","CUSTOMER_LON","TERMINAL_LAT","TERMINAL_LON"]
        if df is not None and not df.empty:
            df.to_csv(out_path, index=False)
        else:
            pd.DataFrame(columns=cols).to_csv(out_path, index=False)
    print(f"Done. Wrote {args.days} daily files to {out_dir}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--customers", type=int, default=5000)
    ap.add_argument("--terminals", type=int, default=1200)
    ap.add_argument("--seed", type=int, default=123)
    args = ap.parse_args()
    simulate(args)
