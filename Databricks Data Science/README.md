# Data Science Fundamentals for Data Analysts using Databricks

# Laod Data
The `.toPandas()` method converts the Spark DataFrame to a Pandas DataFrame.
```
spark_df = spark.read.table("tablename")
pandas_df = park_df.toPandas()
```

```
df = spark.sql('SELECT * FROM tablename').toPandas()
```

<br>

# Convert to Spark DataFrames

```
sdf = spark.createDataFrame(df)
```

<br>

# Save data to SQL

```
train_sdf.write.format("delta").mode("overwrite").save("/dsfda/ht_users_train")
spark.sql(
  "CREATE TABLE IF NOT EXISTS dsfda.ht_users_train USING DELTA LOCATION '/dsfda/ht_users_train'"
)
```

<br>

