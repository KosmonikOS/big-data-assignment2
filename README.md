# Big Data Assignment 2

## How to run

### Step 1: Install prerequisites
- Docker
- Docker compose

### Step 2: Clone the repository
```bash
git clone https://github.com/KosmonikOS/big-data-assignment2
```

### Step 3: Run the containers
```bash
docker compose up 
```
This will create 3 containers, a master node and a worker node for Hadoop, and Cassandra server. The master node will run the script `app/app.sh` as an entrypoint.

### Step 4: Run the search engine
1. Connect to the master node and run the search engine
```bash
docker exec -it cluster-master bash
```
2. Wait until the indexing is complete and example query outputs are visible in the console.

3. Run the search engine
```bash
python3 query.py "This is my query!"
```