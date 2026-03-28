#!/bin/bash
set -e

# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Upgrade pip to the latest version
pip install --upgrade pip setuptools wheel
# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
rm -f .venv.tar.gz
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh

# Run the indexer
bash index.sh

# Run the ranker
bash search.sh "this is a query!"
