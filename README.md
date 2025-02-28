# Design the HashTag Service

## All possible approaches

Implement 5 approaches to count post per hashtag
- Naive (count++) for every event
- Naive batching (batch on server in a hashmap and then write to database)
- Efficient batching with minimizing stop-the-world usng deep-copy
- Efficient batching with minimizing stop-the-world using two-maps
- Kafka adapter pattern to re-ingest the post hashtags partitioned by hashtag


## TODO
1. Artillery for load testing.
2. Setup kafka brokers using docker compose.
3. Measure the number of writes on the database in each of the above approaches - We can try doing this by using artillery
