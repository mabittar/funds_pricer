<img src="https://github.com/damiancipolat/Redis_PUBSUB_node/blob/master/doc/logo.png?raw=true" width="150px" align="right" />

# Web Scraper distributed using redis PUB/SUB and threads

An example of scraper using redis pub/sub features with docker and python + threads.

## Stack:

Our stack will be:

- Python 3.10 + FastAPI
- Multithread and asyncio loop
- Redis native client.
- Redis Timeseries and PubSub
- Docker + docker-compose

## Architecture:

There are three main blocks.

- **Api rest as Pricer**: Receive funds CNPJ to scrap and publish into the redis pubsub.
- **Queues**: Different instances of the same script running in parallel, using asyncio loop to consume messages
  published to redis pubsub in a specific Topic. Acting as consumer group

## Why REDIS

At my current work, the database is a proprietary NoSQL database system. So I took a time to study little more
about that. I prefer study developing something, with hands on the problem.

Redis is largely recognized as the most efficient NoSQL solution to manage rich data structures,
providing the data source to power stateless back-end app servers. What a lot of people don't know
is that it also has a number of characteristics that also make it a prime candidate to develop high-performance
networking solutions with. Probably its most noteworthy fea ture in this area to date, is its built-in
Publishing / Subscribe / Messaging support which enables a new range of elegant comet-based and high
performance networking solutions to be developed.

### REDIS as main database

Redis is also know to be very fast database, but why?

- Redis is a RAM-based database. RAM access is at least 1000 times faster than random disk access.
- Redis leverages IO multiplexing and single-threaded execution loop for execution efficiency.
- Redis leverages several efficient lower-level data structures.

### REDIS as Pub/Sub

I'm already using Redis as main database, and aside for data storage, Redis also can be used as Pub/Sub.
Redis Pub/Sub is designed for speed (low latency), but only with low numbers of subscribers —subscribers don't poll
and while subscribed/connected are able to receive push notifications very quickly from the Redis broker

Messages sent clients to the channel will be pushed by Redis to all the subscribed clients.

A client subscribed the channel should not issue commands, although it can subscribe and unsubscribe to and from other
channels.
The replies to subscription and unsubscribing operations are sent in the form of messages, so that the client can just
read a coherent
stream of messages where the first element indicates the type of message.

Using Redis as PubSub have a huge problem, you will not be able to act a message or retry keep the message in the queue,
wanting for act
if something goes wrong, there no module like fire-and-forget.

### Pub - Sub Design:

All the server and workers run into a docker container, for the queue script I used docker-compose in scale mode.

<img src="https://github.com/damiancipolat/Redis_PUBSUB_node/blob/master/doc/pub-sub-redis.png?raw=true" align="center" />

In this diagram, we are focusing the events architecture to create a async flow using events, I'm using the pub/sub *
*channel** as event queue.

#### Pub/Sub

- Pub/Sub stands for Publisher/Subscriber, is an architecture pattern that allows services to communicate asynchronously
  thus provides a way to build decoupled micro-services.
- Publisher and Subscriber are also known as Producer and Consumer. The Producers and Consumers doesn't need to know
  about each other.
- Topic - is where messages are stored and subscribers can subscribe and receive data from the topic.

## How to use it

### Using Docker and Docker-compose

From the project's root folder.

```shell
docker-compose down
docker-compose up --build -d --force-recreate
docker-compose up --scale queue=3 -d
docker logs --follow funds_pricer_queue_1
```

### Redis Logs and Monitor

```shell
docker exec -it redis redi-cli

127.0.0.1:6379> MONITOR
```

### Push manual message

```shell
docker exec -it redis redi-cli
PUBLISH "fund_parser" "{\"document\": \"18993924000100\", \"fund_pk\": \"132922\", \"month_year\": \"12/2022\", \"message_id\": \"6db8704d-5b97-4c95-bb53-b052b48ed531\", \"acked\": false}"
```

Will trigger the monitor from Redis Container Server.