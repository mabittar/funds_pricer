import aioredis as redis
import asyncio


async def test_redis():
    try:
        db = await redis.Redis(
            host="localhost",
            port=6379,
            decode_responses=True,
            encoding="utf-8"
        )
        print(type(db))
        async with db.client() as conn:
            result = await conn.execute_command("set", "my-key", "some value")
            assert result is True
            print("connection is working!")
    except (redis.ConnectionError, redis.BusyLoadingError, redis.ResponseError) as e:
        print(e)


async def main():
    await test_redis()


if __name__ == '__main__':
    print("This script check for redis connection")
    print("Open another terminal and type: 'redis-cli' -> 'MONITOR'")
    asyncio.run(main())
