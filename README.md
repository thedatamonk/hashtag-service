Design the HashTag Service
===

<!--ts-->
* [Design the HashTag Service](#design-the-hashtag-service)
* [Problem Statement](#problem-statement)
* [Requirements](#requirements)
   * [Core Requirements](#core-requirements)
   * [High Level Requirements](#high-level-requirements)
   * [Micro Requirements](#micro-requirements)
* [Output](#output)
   * [Design Document](#design-document)
   * [Prototype](#prototype)
      * [Recommended Tech Stack](#recommended-tech-stack)
      * [Keep in mind](#keep-in-mind)
* [Outcome](#outcome)
   * [You'll learn](#youll-learn)
* [Share and shoutout](#share-and-shoutout)
<!--te-->

# Problem Statement

Say you own a social network in which people upload photos and with each upload people can provide a list of HashTags as part of the caption. Build a service that manages the hashtags along with it it helps us render a HashTag page that shows

 - hashtag
 - total number of photos posted with that HashTag
 - top 50 photos with that HashTag

This service has to handle 5 million photo uploads every hour and each photo has ~8 hashtags.

![Relog The HashTag Service](https://user-images.githubusercontent.com/4745789/139570503-5b213da5-3a74-4187-9843-c3f718abe0e4.png)

# Requirements

<!--rs-->
*The problem statement is something to start with, be creative and dive into the product details and add constraints and features you think would be important.*
<!--re-->

## Core Requirements

 - **extract** and **manage** HashTags from all the uploaded photos
 - **5 million** photos uploaded every hour
 - efficiently drive the HashTag page that shows
     - the hashtag
     - the number of photos with that hashtags
     - top 50 photos for that hashtag

##  High Level Requirements
<!--hs-->
- make your high-level components operate with **high availability**
 - ensure that the data in your system is **durable**, not matter what happens
 - define how your system would behave while **scaling-up** and **scaling-down**
 - make your system **cost-effective** and provide a justification for the same
 - describe how **capacity planning** helped you made a good design decision 
 - think about how other services will interact with your service
<!--he-->

##  Micro Requirements
<!--ms-->
- ensure the data in your system is **never** going in an inconsistent state
 - ensure your system is **free of deadlocks** (if applicable)
 - ensure that the throughput of your system is not affected by **locking**, if it does, state how it would affect
<!--me-->

# Output

## Design Document
<!--ds-->
Create a **design document** of this system/feature stating all critical design decisions, tradeoffs, components, services, and communications. Also specify how your system handles at scale, and what will eventually become a chokepoint.

Do **not** create unnecessary components, just to make design look complicated. A good design is **always simple and elegant**. A good way to think about it is if you were to create a spearate process/machine/infra for each component and you will have to code it yourself, would you still do it?
<!--de-->

## Prototype

To understand the nuances and internals of this system, build a prototype that

- build a small prototype that upload photo upload
   - extracts the hashtags
   - stores them in a database
   - updates the count of photos for each hashtag

###  Recommended Tech Stack

This is a recommended tech-stack for building this prototype

|Which|Options|
|-----|-----|
|Language|Golang, Java, C++|
|Database|pick your favourite|

###  Keep in mind

These are the common pitfalls that you should keep in mind while you are building this prototype

- the number of writes on the database would explode at scale
- count++ is not atomic by default

# Outcome

##  You'll learn

- managing counters at scale
- designing loosely coupled architecture
- designing efficient service while keeping in mind User Experience



# TODO
1. We will need mongodb to store hashtag info
   - hashtags collection
   - posts collection
2. rabbitmq for asynchronous computing
3. golang for api building
4. [TODO] artillery for load testing
5. Learn kafka and use it instead of rabbitmq.
6. Learn more about kafka and zookeeper. And what's the intent of different providers like bitnami
# All possible approaches

Implement 5 approaches to count post per hashtag
- Naive (count++) for every event
- Naive batching (batch on server and then write to database)
- Efficient batching with minimizing stop-the-world usng deep-copy
- Efficient batching with minimizing stop-the-world using two-maps
- Kafka adapter pattern to re-ingest the post hashtags partitioned by hashtag
- Measure the number of writes on the database in each of the above approaches