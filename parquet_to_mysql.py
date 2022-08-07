import pyarrow.parquet as pq
import pandas as pd
import sys

def main():
    if len(sys.argv) != 3:
        return

    trips = pd.read_parquet(f"{sys.argv[1]}_{sys.argv[2]}_taxidata.parquet")
    trips.to_csv(f"{sys.argv[1]}_{sys.argv[2]}_taxidata.csv")

main()