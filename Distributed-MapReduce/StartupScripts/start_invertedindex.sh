#!/bin/bash

python3 datastore.py ipconfig.json &
python3 invertedindex_mapper.py ipconfig.json &
python3 invertedindex_reducer.py ipconfig.json &
python3 master.py ipconfig.json $1 &
