import pandas as pd
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
    for year in ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022"]:
        for month in ["01", "02", "03", "04", "05", "07", "08", "09", "10", "11", "12"]:
            if exists(f"{year}_{month}_taxidata.parquet"):
                trips = pd.read_parquet(f"{year}_{month}_taxidata.parquet")

                engine = create_engine(f"mysql+pymysql://{uname}:{pwd}@{hostname}/{dbname}")
                trips.to_sql(con=engine, name=f'{year}_{month}', if_exists='replace')

main()