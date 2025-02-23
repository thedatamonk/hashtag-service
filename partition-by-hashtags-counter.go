package main

// in this approach, we will have two brokers
// one broker (partitioned by user_id) will be responsible for receiving posts and extracting hashtags per post
// for each hashtag extracted from the post, send another event to  the other broker (parttitioned by hashtag)
// the consumer of this broker will accumulate the number of posts per hashtag and then write it in bulk to mongodb
