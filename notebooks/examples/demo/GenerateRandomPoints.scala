// Databricks notebook source
// MAGIC %md <img src="/files/milos_colic/mosaic_logo.png">

// COMMAND ----------

// MAGIC %md
// MAGIC In order for Mosaic to correctly run in your notebook we need to set up few things. </br>
// MAGIC For convenience we have packaged this as a set of notebooks that you can easily run from a cell using %run. </br>
// MAGIC The 'EnableMosaic' notebook is avaialbe at the public repo. </br>
// MAGIC The 'EnableMosaic' notebook will:
// MAGIC <ul>
// MAGIC   <li>Intialize MosaicContext</li>
// MAGIC   <li>Setup utility methods for pretty printing</li>
// MAGIC   <li>Perform a necessary patch for generator expressions</li>
// MAGIC </ul>

// COMMAND ----------

// MAGIC %run "./../../setup/EnableMosaic"

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC In this notebook we will prepare some random point data tha we can use to demonstarate Point in Polygon joins. </br>
// MAGIC For that purpose we will use our randomized neighbourhood data. </br>

// COMMAND ----------

val neighbourhoodIndices = spark.read.table("randomized_neighbourhoods_indexed_3M")
  .drop("geometry").withColumn("h3_id", col("h3")).drop("h3")

// COMMAND ----------

neighbourhoodIndices.withColumn("wkt", st_astext(col("wkb"))).drop("wkb").createOrReplaceTempView("neighbourhoodIndices")

// COMMAND ----------

// MAGIC %python
// MAGIC %%mosaic_kepler
// MAGIC "neighbourhoodIndices" "h3_id" "h3" 5000

// COMMAND ----------

neighbourhoodIndices.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Our data contains about 630M index rows that contain either core or border chips.

// COMMAND ----------

// MAGIC %md
// MAGIC We will extract centroid points of each border chip and extract the coordinates. </br>
// MAGIC We will then generate an id for each row. </br>
// MAGIC We will explode said data 10K times. </br>
// MAGIC Finally we will translate all the generated points by random offsets. </br>
// MAGIC This will yield us about 22B points that we can use to generate some join operations.

// COMMAND ----------

val pointsData = neighbourhoodIndices
  .withColumn("centroid", st_centroid2D(col("wkb")))
  .select(
    monotonically_increasing_id().alias("id"),
    col("centroid"),
    col("centroid").getItem("x").alias("c_x"),
    col("centroid").getItem("y").alias("c_y"),
    rand().alias("measurement")
  )
  .withColumn("exp_id_1", explode(array((0 until 100).map(lit): _*))) // replicate data 100 times
  .withColumn("translated", st_translate(st_point(col("c_x"), col("c_y")), (lit(0.5) - rand()) * 0.05, (lit(0.5) - rand()) * 0.05 ))
  .where("centroid is not null")
  .withColumn("x", st_xmax(col("translated")))
  .withColumn("y", st_ymax(col("translated")))
  .withColumn("h3_id", point_index(col("y"), col("x"), lit(10))) // we will add a function to determine resolution based on one of the indices from original data, for now it is just a constant

// COMMAND ----------

displayMosaic(pointsData)

// COMMAND ----------

pointsData.select("x", "y", "h3_id").createOrReplaceTempView("points_data")

// COMMAND ----------

// MAGIC %md
// MAGIC We will visualise our points data and their H3 indices to verify we have enough spread.

// COMMAND ----------

// MAGIC %python
// MAGIC %%mosaic_kepler
// MAGIC "points_data" "h3_id" "h3" 5000

// COMMAND ----------

pointsData.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Finally we will write down the generated points and zorder them in the same way we did with polygons.

// COMMAND ----------

pointsData.write.format("delta").mode("overwrite").save("/Users/milos_colic/mosaic/demo/randomized_points")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE randomized_points
// MAGIC using delta
// MAGIC LOCATION "/Users/milos_colic/mosaic/demo/randomized_points"

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE randomized_points ZORDER BY (x, y, h3_id)

// COMMAND ----------


