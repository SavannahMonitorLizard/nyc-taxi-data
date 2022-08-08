import pandas as pd
import sys
from os.path import exists
from sqlalchemy import create_engine
import json

with open("config.json") as json_file:
    config = json.load(json_file)

    hostname = config["ip"]
    dbname = config["dbname"]
    uname = config["username"]
    pwd = config["password"]

year = ""
month = ""

def main():
    if len(sys.argv) != 3:
        return
    year = sys.argv[1]
    month = sys.argv[2]

    if exists(f"{year}_{month}_taxidata.parquet"):
        trips = pd.read_parquet(f"{year}_{month}_taxidata.parquet", safe=False)

        engine = create_engine(f"mysql+pymysql://{uname}:{pwd}@{hostname}/{dbname}")
        trips.to_sql(con=engine, name=f'{year}_{month}', if_exists='replace')

main()