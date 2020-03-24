#!/bin/bash

python3 datastore.py ipconfig.json &
python3 wordcount_mapper.py ipconfig.json &
python3 wordcount_reducer.py ipconfig.json &
python3 master.py ipconfig.json $1 &

