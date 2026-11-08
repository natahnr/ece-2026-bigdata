import re
import time
from operator import add
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# --- Création et partitions
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print(users_lines.getNumPartitions(), orders_lines.getNumPartitions())
print(orders_lines.repartition(8).getNumPartitions())

# --- Lazy evaluation
header = orders_lines.first()
orders_data = orders_lines.filter(lambda l: l != header and l.strip() != "")
print(orders_data.toDebugString().decode())
print(orders_data.count())

# --- Users multiligne
UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
print(users_lines.count())
users_records = users_lines.filter(lambda l: UUID_AT_START.match(l) is not None)
print(users_records.count())

# --- Parsing des orders
def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {"order_id": order_id, "user_id": user_id, "date": date,
            "quantity": int(quantity), "product": product.strip().lower()}

orders_rdd = orders_data.map(parse_order).cache()
print(orders_rdd.take(3))

# --- reduceByKey vs groupByKey
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
user_quantity_grouped = (orders_rdd.map(lambda o: (o["user_id"], o["quantity"]))
                         .groupByKey().mapValues(sum))
print(user_quantity.take(5))
print(user_quantity_grouped.take(5))

# --- Déduplication
def most_recent(a, b):
    return a if a["date"] > b["date"] else b

orders_dedup = (orders_rdd.map(lambda o: (o["order_id"], o))
                .reduceByKey(most_recent).values())
print(orders_rdd.count(), orders_dedup.count())

# --- Jointure
def parse_user_for_join(line):
    f = line.split(",")
    return f[0], f[1]

users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)
print(enriched.take(3))

# --- Exercice 1
user_stats = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0), lambda acc, q: (acc[0] + 1, acc[1] + q), lambda a, b: (a[0] + b[0], a[1] + b[1]))
print(user_stats.mapValues(lambda s: s[1] / s[0]).take(5))

# --- Exercice 2
fake = sc.parallelize([{"order_id": "x1", "user_id": "fake-user", "date": "2020-01-01",
                        "quantity": 1, "product": "test"}])
left = (orders_rdd.union(fake).map(lambda o: (o["user_id"], o))
        .leftOuterJoin(users_kv))
print(left.filter(lambda kv: kv[1][1] is None).count())

# --- Exercice 3
df = spark.createDataFrame(orders_rdd)
df.groupBy("user_id").sum("quantity").show(5)
df.groupBy("user_id").sum("quantity").explain()
print(user_quantity.toDebugString().decode())

# --- Exercice 4
def timeit(label, rdd):
    t = time.perf_counter()
    rdd.count()
    print(label, round(time.perf_counter() - t, 3), "s")

timeit("orders_rdd cache", orders_rdd)
timeit("user_quantity cache", user_quantity)
orders_rdd.unpersist()
timeit("orders_rdd sans cache", orders_rdd)
timeit("user_quantity sans cache", user_quantity)

spark.stop()
