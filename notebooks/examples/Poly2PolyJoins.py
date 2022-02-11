# Databricks notebook source
# MAGIC %md <img src="/files/milos_colic/mosaic_logo.png">

# COMMAND ----------

# DBTITLE 1,Enable Mosaic
# MAGIC %run "./../setup/EnableMosaic"

# COMMAND ----------

# DBTITLE 1,Spark SQL functions import
import pyspark.sql.functions as F

# COMMAND ----------

username = "milos.colic" #please update with a correct user
silver_data_location = f"Users/{username}/geospatial/workshop/data/silver"

# COMMAND ----------

# DBTITLE 1,Fetch Polygons 1 Data
polygons1 = spark.read.format("delta").load(f"/{silver_data_location}/h3/neighbourhoods/random/dataset_1_decomposed")
displayMosaic(polygons1)

# COMMAND ----------

# DBTITLE 1,Fetch Polygons 2 Data
polygons2 = spark.read.format("delta").load(f"/{silver_data_location}/h3/neighbourhoods/random/dataset_2_decomposed")
displayMosaic(polygons2)

# COMMAND ----------

import shapely

@udf("boolean")
def st_intersects(pairs):
  from shapely import wkb as wkb_io
  
  def geom_intersect(pair):
    (left, right) = pair
    if left["chip"] is None or right["chip"] is None:
      return False
    left_geom = wkb_io.loads(bytes(left["chip"]))
    right_geom = wkb_io.loads(bytes(right["chip"]))
    return left_geom.intersects(right_geom)
  
  flags = [~p["left"]["is_border"] or ~p["right"]["is_border"] for p in pairs]
  if sum(flags) > 0:
    return True
  else:
    for p in pairs:
      if p["left"]["is_border"] and p["right"]["is_border"]:
        if geom_intersect(p):
          return True
    return False


# COMMAND ----------

from pyspark.sql import functions as F

def intersection_join(left, right):
  intersections = left.join(
    right,
    on = ["h3"],
    how = "inner"
  ).groupBy(
    left["id"],
    right["id"]
  ).agg(
    F.collect_set(F.struct(left["chip"].alias("left"), right["chip"].alias("right"))).alias("matches")
  ).withColumn(
    "intersects", 
      F.array_contains(F.col("matches.left.is_border"), F.lit(False)) | 
       F.array_contains(F.col("matches.right.is_border"), F.lit(False)) |
       st_intersects(F.col("matches"))
  )
  return intersections

# COMMAND ----------

left = polygons1.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(10000)
right = polygons2.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(10000)

intersections = intersection_join(left, right)
display(intersections)

# COMMAND ----------

left = polygons1.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(100000)
right = polygons2.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(100000)

intersections = intersection_join(left, right)
intersections.count()

# COMMAND ----------

left = polygons1.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(500000)
right = polygons2.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(500000)

intersections = intersection_join(left, right)
intersections.count()

# COMMAND ----------

left = polygons1.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(1000000)
right = polygons2.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null").limit(1000000)

intersections = intersection_join(left, right)
intersections.count()

# COMMAND ----------

left = polygons1.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null")
right = polygons2.withColumn("chip", F.explode("chips")).drop("chips").withColumn("h3", F.col("chip.index")).where("chip is not null")

intersections = intersection_join(left, right)
intersections.count()

# COMMAND ----------


