
|        |                                |
| ------ | ------------------------------ |
| Track: | **DS-02**                      |
| Name:  | Nikita Tiurkov                 |
| Email: | n.tiurkov@innopolis.university |

## Assignment 2 **Introduction to Big Data**

### Running the search engine

#### Step 1: Install prerequisites
- Docker
- Docker Compose

#### Step 2: Clone the repository
```bash
git clone https://github.com/KosmonikOS/big-data-assignment2
```

#### Step 3: Download the dataset
For the purpose of the assignment I use the specified [dataset from Kaggle](https://www.kaggle.com/datasets/jjinho/wikipedia-20230701?select=a.parquet). Before proceeding to the next step, please download it and place the `a.parquet` file in the `/app` directory on the local machine.

#### Step 4: Run the containers
```bash
docker compose up 
```
This will create 3 containers (a master and a worker node for Hadoop, and a Cassandra server). The master node will run the script `app/app.sh` as an entrypoint (it will index the data and run the example search query).

#### Step 5: Run the retrieval
After the indexing is complete, you can run the retrieval by executing the following commands:
```bash
docker exec -it cluster-master bash
chmod +x search.sh
./search.sh "This is my query!"
```