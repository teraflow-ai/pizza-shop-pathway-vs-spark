# Pizza Shop - Pathway vs Spark

Code compliment to the blog post on this topic.

Infra and generated data used from https://github.com/startreedata/pizza-shop-demo.

## Spin Up Infra

**X86**:

```bash
docker-compose \
  -f docker-compose-base.yml \
  -f docker-compose-pinot.yml \
  -f docker-compose-dashboard-enriched-quarkus.yml \
  up
```

**Arm64**:
```bash
docker-compose \
  -f docker-compose-base.yml \
  -f docker-compose-pinot-m1.yml \
  -f docker-compose-dashboard-enriched-quarkus.yml \
  up
```

## Run Spark Analysis

### Customer Stats

```base
spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 \
        spark/most_valuable_customers_stats.py
```

### Total Pizzas Sold

```base
spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 \
        spark/count_num_pizzas_sold.py
```
