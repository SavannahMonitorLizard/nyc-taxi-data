# nyc-taxi-data
downloads taxi data from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page and converts it to a MySQL database

download.sh takes 4 digit year argument and 2 digit month argument ex. download.sh 2022 01
downloadall.sh downloads all available data as of April 2022

parquet_to_mysql.py takes 4 digit year argument and 2 digit month argument ex py parquet_to_mysql.py 2022 01
pq_to_mysql.py will create table for all years and months downloaded

config.json must be set up before use
```
{
    "ip": "",
    "dbname": "",
    "username": "",
    "password": ""
}
```