#!/bin/bash

# create raw data directory
mkdir -p data/raw

# download raw data
# 2020
wget -O data/raw/scada_2020.zip https://zenodo.org/records/5841834/files/Kelmarsh_SCADA_2020_3086.zip?download=1

# unzip raw data
unzip data/raw/scada_2020.zip -d data/raw/scada_2020
