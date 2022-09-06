import pandas as pd
import sys
from os.path import exists
from sqlalchemy import create_engine
import json
import pyarrow.parquet as pq
import pyarrow

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
        trips = pq.read_table(f'{year}_{month}_taxidata.parquet', 
        coerce_int96_timestamp_unit="ns")
        try:    
            trips = trips.to_pandas()
        except Exception as e:
            print(e.__doc__)
            print(f"Error converting {year}_{month} to pandas, exiting")
            return
        engine = create_engine(f"mysql+pymysql://{uname}:{pwd}@{hostname}/{dbname}")
        trips.to_sql(con=engine, name=f'{year}_{month}', if_exists='replace')

main()