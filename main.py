from pyspark.sql.functions import col,lit,create_map,collect_list

# Picking latest setting from the data set
# funtion will be running in parallel on all the partitions
def pick_latest_setting(partitionData):
    for row in partitionData:
        latest_settings = {}
        hash_table = {}
        for item in row.settings:
            if item['name'] in hash_table:
                if hash_table[item['name']] < item['timestamp']:
                    latest_settings[item['name']] = item['value']
                else:
                    pass
            else:
                hash_table[item['name']] = item['timestamp']
                latest_settings[item['name']] = item['value']
        yield [row.id, latest_settings]

# reading the file daily
a = {"header" : "true"}
df = spark.read.options(**a).csv("s3://atl-ai-zone-cdr-stg/essot/test/mayank/SampleValues.csv")

# creating a map of every setting
df_new = df.withColumn("setting",create_map(
        lit("name"),col("name"),
        lit("value"),col("value"),
          lit("timestamp"),col("timestamp")
        )).drop("name","value", "timestamp")

#Reducing the size of the data and combining the setting per id
df_agg = df_new.groupBy(col("id")).agg(collect_list(col("setting"))).withColumnRenamed("collect_list(setting)", "settings")

# pick the latest setting from each partition
df_latest = df_agg.rdd.mapPartitions(pick_latest_setting).toDF(["id", "setting"])

# ACTION: showing the results or modify the code to write into Hive per day partition
df_latest.show(truncate=False)