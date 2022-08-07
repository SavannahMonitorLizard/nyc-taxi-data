#!/bin/bash
curl https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_$1-$2.parquet -o $1_$2_taxidata.parquet