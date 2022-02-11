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

spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", "true")
import org.apache.spark.sql.functions._

// COMMAND ----------

val pointsData = spark.read.table("randomized_points")

// COMMAND ----------

displayMosaic(pointsData)

// COMMAND ----------

pointsData.count()

// COMMAND ----------

val polygonsData = spark.read.table("randomized_neighbourhoods_indexed_3M")

// COMMAND ----------

displayMosaic(polygonsData.withColumn("geometry", col("wkb")))

// COMMAND ----------

polygonsData.count()

// COMMAND ----------

// MAGIC %md
// MAGIC With our data read we can do some spatial joins based on Point in Polygon relationship. </br>
// MAGIC These joins will leverage our h3 indices and skip st_contains call whenever possible. </br>
// MAGIC We will compute some aggregates on top of the join so that rows are materialized and shuffled. </br>
// MAGIC Since our join space is massive 630Mx23B and there are many to many matchings we are limitting join results to "just" 100M. </br>
// MAGIC Note: since wholeStageCodegen isnt fully working limits are explict and are not pushed down (this will be fixed soon). </br>
// MAGIC Once WSCG is working and enabled we will no longer need manual limits for displays.

// COMMAND ----------

val chippedJoin = polygonsData
  .withColumn("poly_id", monotonically_increasing_id())
  .join(
    pointsData,
    polygonsData.col("h3") === pointsData.col("h3_id"),
    "inner"
  ).where(
    polygonsData.col("is_core") || st_contains(polygonsData.col("wkb"), st_point(pointsData.col("x"), pointsData.col("y")))
  )
  .limit(100*1000*1000) // This is a 23.5B points x 3M polygons join with many 2 many relationships
  // limit the number of outputs so the job doesnt take too much time 

val countPerChip = chippedJoin.groupBy(
    "h3", "poly_id"
  ).agg(
    count("*").alias("count"),
    first(col("wkb")).alias("geometry")
  )

// COMMAND ----------

displayMosaic(chippedJoin.drop("geometry", "translated"))

// COMMAND ----------

displayMosaic(countPerChip)

// COMMAND ----------

// MAGIC %md
// MAGIC Finally we can visualise some of our aggregates via Mosaic Kepler magics.

// COMMAND ----------

countPerChip.select(col("h3").alias("h3_id"), col("count"), st_astext(col("geometry")).alias("wkt"), col("poly_id")).createOrReplaceTempView("counts_per_chip")

// COMMAND ----------

// MAGIC %python
// MAGIC %%mosaic_kepler
// MAGIC "counts_per_chip" "h3_id" "h3"

// COMMAND ----------

val polygonsWithID = polygonsData
  .withColumn("poly_id", monotonically_increasing_id())
val leftPolygons = polygonsWithID
  .withColumnRenamed("is_core", "left_is_core")
  .withColumnRenamed("wkb", "left_wkb")
  .withColumnRenamed("poly_id", "left_poly_id")
val rightPolygons = polygonsWithID
  .withColumnRenamed("is_core", "right_is_core")
  .withColumnRenamed("wkb", "right_wkb")
  .withColumnRenamed("poly_id", "right_poly_id")

val chippedIntersectionJoin = leftPolygons
  .join(
    rightPolygons,
    Seq("h3"),
    "inner"
  ).where(
    (col("left_is_core") ||  col("right_is_core")) // relaxed condition 
  )
  .groupBy(
    "left_poly_id", "right_poly_id"
  )
  .agg(
    first("left_wkb").alias("left_wkb"),
    first("right_wkb").alias("right_wkb")
  )
  .limit(100000000) // This is a 23.5B points x 3M polygons join with many 2 many relationships
  // limit the number of outputs so the job doesnt take too much time 

// COMMAND ----------

displayMosaic(chippedIntersectionJoin)

// COMMAND ----------


