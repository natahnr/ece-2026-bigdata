import argparse
import datetime

from faker import Faker
from faker.providers import DynamicProvider

from dataset_users import users_generate
from serialize import serialize

fake = Faker()
# Generate the same data set
Faker.seed(42)
products = DynamicProvider(
    provider_name="producs",
    elements=["bread", "brioche", "cookie", "croissant", "donuts", "dring"],
)
fake.add_provider(products)


def generate_orders(count=100, count_users=50, output=""):
    orders = []
    for user in users_generate(count_users):
        date_start = datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.UTC).timestamp()
        date_end = date_start + 60 * 60
        for i in range(count):
            # User generation
            order = {
                "uuid": fake.uuid4(),
                "user_uuid": user["uuid"],
                "date": fake.date_time_between(
                    datetime.datetime.fromtimestamp(date_start, tz=datetime.UTC),
                    datetime.datetime.fromtimestamp(date_end, tz=datetime.UTC),
                ),
                "quantity": fake.pyint(min_value=1, max_value=5),
                "product": fake.producs(),
            }
            orders.append(order)
            # Generation period increment of 1 hour
            date_start = date_end
            date_end = date_start + 60 * 60
    serialize(orders, output)


def main():
    parser = argparse.ArgumentParser(prog="Orders generator")
    parser.add_argument(
        "-C",
        "--count_min",
        help="Minimum number of orders to generate per user.",
        type=int,
        default=0,
    )
    parser.add_argument(
        "-c",
        "--count_max",
        help="Maximum number of orders to generate per user.",
        type=int,
        default=100,
    )
    parser.add_argument(
        "-d",
        "--data_from",
        help="Date from",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output format.",
        default="json",
        choices=["csv", "json", "jsonline"],
    )
    parser.add_argument(
        "-u", "--count_users", help="Number of users to generate.", type=int, default=50
    )
    args = parser.parse_args()
    generate_orders(args.count_max, args.count_users, args.output)


if __name__ == "__main__":
    main()