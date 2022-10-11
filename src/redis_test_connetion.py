from redis_om import get_redis_connection
import datetime

from redis_om import HashModel


class Customer(HashModel):
    first_name: str
    last_name: str
    email: str
    join_date: datetime.date
    age: int
    bio: str


andrew = Customer(
    first_name="Andrew",
    last_name="Brookins",
    email="andrew.brookins@example.com",
    join_date=datetime.date.today(),
    age=38,
    bio="Python developer, works at Redis, Inc."
)
# This Redis instance is tuned for durability.
REDIS_DATA_URL = "redis://localhost:6380"
try:
    db = get_redis_connection(url=REDIS_DATA_URL, decode_responses=True)
    type(db)
    print("connection done!")
    print(andrew.pk)
    andrew.save()
except Exception as e:
    print(e)

