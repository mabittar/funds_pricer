import aioredis as redis

# This Redis instance is tuned for durability.
REDIS_DATA_URL = "redis://localhost:6380"
try:
    db = redis.Redis(
        host="localhost",
        port=15000,
        decode_responses=True,
        encoding="utf-8"
    )
    type(db)
    print("connection done!")
    await db.execute_command(
        'CREATE',
        'DUPLICATE_POLICE',
        'first',
        "PRICER_TEST",
        labels={'NAME': "Fund Test",
                'ACTIVE': True,
                'FUND_ID': 123456,
                'RELEASED_ON': "1900/01/01",
                'LAST_QUERY_DATE': "2022/11/10"
                }
    )
except (redis.ConnectionError, redis.BusyLoadingError) as e:
    print(e)
