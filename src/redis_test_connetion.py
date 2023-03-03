import aioredis as redis
import asyncio


TEST_CHANNEL = "test_channel"


async def connect_redis(set_key=False):
    try:
        db = await redis.Redis(
            host="localhost",
            port=6379,
            decode_responses=True,
            encoding="utf-8"
        )
        print(type(db))
        if set_key:
            async with db.client() as conn:
                result = await conn.execute_command("set", "my-key", "some value")
                assert result is True
                print("connection is working!")
    except (redis.ConnectionError, redis.BusyLoadingError, redis.ResponseError) as e:
        print(e)
    return db


async def publisher_redis_stream():
    db = await connect_redis()
    count = 0
    while count < 10:
        try:
            data = {
                "document": "12345678900",
                "acked": 0,
                "message_id": count
            }
            resp = await db.xadd(TEST_CHANNEL, data)
            print(resp)
            count += 1
        except (redis.ConnectionError, redis.BusyLoadingError, redis.ResponseError) as e:
            print(e)
    await db.close()


async def consume_redis_stream():
    last_id = 0
    sleep_ms = 5000
    db = await connect_redis()
    while True:
        try:
            if await db.xlen(TEST_CHANNEL):
                resp = await db.xread(
                    {TEST_CHANNEL: last_id}, count=1, block=sleep_ms
                )
                if resp:
                    key, messages = resp[0]
                    last_id, data = messages[0]
                    print("REDIS ID: ", last_id)
                    print("      --> ", data)
            else:
                await asyncio.sleep(5)
        except (redis.ConnectionError, redis.BusyLoadingError, redis.ResponseError) as e:
            print(e)



async def main():
    db = await connect_redis(set_key=True)
    await db.close()
    await publisher_redis_stream()
    await consume_redis_stream()


if __name__ == '__main__':
    print("This script check for redis connection")
    print("Open another terminal and type: 'redis-cli' -> 'MONITOR'")
    asyncio.run(main())
