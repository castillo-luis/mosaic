// Databricks notebook source
// MAGIC %md <img src="https://raw.githubusercontent.com/databricks/mosaic/main/src/main/resources/mosaic_logo.png?token=GHSAT0AAAAAABORRJ7JOINVYPW373XNVTDAYPGRZJA">

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
// MAGIC In this notebook we will use Mosaic to generate some random shape data. </br>
// MAGIC For input data we will use neighbourhoods.geojson file that contains NYC neighbourhood polygons. </br>
// MAGIC Notice how easier is to load geojson files with Mosaic (no need for geopandas + toSparkDf tricks). </br>
// MAGIC This allows us to read 1000s of geojson files in parallel if a need is present.

// COMMAND ----------

val neighbourhoodsLocation = "/Users/milos_colic/neighborhoods.geojson.txt"

val geoJsonDf = spark.read.format("json")
  .load(neighbourhoodsLocation)
  .withColumn("geometry", st_geomfromgeojson(to_json(col("geometry"))))
  .select("properties.*", "geometry")
  .where("geometry is not null")

// COMMAND ----------

displayMosaic(geoJsonDf)

// COMMAND ----------

// MAGIC %md
// MAGIC We will prepare our dataset so that we can display it in Mosaic Kepler magic. </br>
// MAGIC At the moment Bindary and Struct data has to be droped before the magic is called. </br>
// MAGIC Also the magic expects a registered tables, so we will create a temp view. 

// COMMAND ----------

geoJsonDf
  .withColumn("centroid", st_centroid2D(col("geometry")))
  .withColumn("h3_id", point_index(col("centroid").getItem("y"), col("centroid").getItem("x"), 8))
  .withColumn("wkt", st_astext(col("geometry")))
  .select("borough", "h3_id", "wkt")
  .createOrReplaceTempView("neighbourhoods")

// COMMAND ----------

// MAGIC %python
// MAGIC %%mosaic_kepler
// MAGIC "neighbourhoods" "h3_id" "h3"

// COMMAND ----------

// MAGIC %md
// MAGIC This looks pretty neat however any benchmark against this type of data would be cheating. </br>
// MAGIC Polygons are perfectly alligned without any overlap. </br>
// MAGIC In order to generate some representability we will randomply translate our geometries and allow for overlaps. </br>
// MAGIC We will also increase cardinality of the data 10K times to generate some considerable volume. </br>

// COMMAND ----------

val randomized = geoJsonDf.repartition()
  .withColumn("exp_id_1", explode(array((0 until 100).map(lit): _*))) // replicate data 100 times
  .withColumn("exp_id_2", explode(array((0 until 100).map(lit): _*))) // replicate data 100 times more
  .repartition(100, col("exp_id_1"))
  .withColumn(
    "geometry", st_translate(col("geometry"), (lit(0.5) - rand()), (lit(0.5) - rand())) // randomly translate all geoms
  )

// COMMAND ----------

randomized
  .withColumn("centroid", st_centroid2D(col("geometry")))
  .withColumn("h3_id", point_index(col("centroid").getItem("y"), col("centroid").getItem("x"), 8))
  .withColumn("wkt", st_astext(col("geometry")))
  .select("borough", "h3_id", "wkt")
  .createOrReplaceTempView("randomized_neighbourhoods")


// COMMAND ----------

// MAGIC %python
// MAGIC %%mosaic_kepler
// MAGIC "randomized_neighbourhoods" "h3_id" "h3"

// COMMAND ----------

displayMosaic(spark.read.table("randomized_neighbourhoods"))

// COMMAND ----------

spark.read.table("randomized_neighbourhoods")
  .withColumn("geometry", convert_to(col("wkt"), "coords"))
  .withColumn("xmin", st_xmin(col("geometry")))
  .withColumn("xmax", st_xmax(col("geometry")))
  .withColumn("ymin", st_ymin(col("geometry")))
  .withColumn("ymax", st_ymax(col("geometry")))
  .write.format("delta").mode("overwrite").save("/Users/milos_colic/mosaic/demo/randomized_neighbourhoods")

// COMMAND ----------

// MAGIC %md
// MAGIC We will reload the table from the storage now. </br>
// MAGIC This will leverage delta metadata at run time and it will benefit for zordering (we havent done it just yet). </br>
// MAGIC The volume we have generated is 3.1M polygons. </br>
// MAGIC Note: the level of details some of these geometries have is respectable (up to 2k vertices per polygon). </br>
// MAGIC This should make our benchmarks robust.

// COMMAND ----------

val randomized_neighbourhoods = spark.read.format("delta").load("/Users/milos_colic/mosaic/demo/randomized_neighbourhoods")

// COMMAND ----------

randomized_neighbourhoods.count()

// COMMAND ----------

// MAGIC %md
// MAGIC For ZORDER opeartion we are required to register our delta table with the metastore. </br>
// MAGIC We will create the table and proceed with the ZORDER. </br>
// MAGIC Note that we will use 5 columns to ZORDER our data. </br>
// MAGIC All of the aforementioned columns are spatial in nature so clustering will remain consistement. </br>
// MAGIC This will allow us to serve queries that leverage Index System (H3) and/or Bounding Box coordinates. 

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE randomized_neighbourhoods_delta
// MAGIC using delta
// MAGIC LOCATION "/Users/milos_colic/mosaic/demo/randomized_neighbourhoods"

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE randomized_neighbourhoods_delta ZORDER BY (xmin, xmax, ymin, ymax, h3_id)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from randomized_neighbourhoods_delta

// COMMAND ----------

// MAGIC %sql
// MAGIC create TEMPORARY view randomized_neighbourhoods_delta_plotable as (
// MAGIC   select wkt, h3_id, borough from randomized_neighbourhoods_delta
// MAGIC )

// COMMAND ----------

// MAGIC %python
// MAGIC %%mosaic_kepler
// MAGIC "randomized_neighbourhoods_delta_plotable" "h3_id" "h3" 1000

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have inspected the geometries and we are happy with how the randomized data looks like, we will index it and store it in delta as well. </br>
// MAGIC We will leverage mosaic_exlode expresion togther with post-write zorder of the data. </br>
// MAGIC This should yield us the best performance. </br>
// MAGIC In order to properly select the resolution of indexing we can use MosaicFrame API that will analyse the geometries for us.

// COMMAND ----------

val metrics = MosaicFrame(randomized_neighbourhoods).setGeom("wkt").getResolutionMetrics() // internal geometry not yet supported for this operation

// COMMAND ----------

display(metrics)

// COMMAND ----------

// MAGIC %md
// MAGIC Based on the ratios above (each line track the ratio between area of a representative geometry vs area of an index at selected resolution). </br>
// MAGIC We can conclude that on resolution 9 most of our geometries will be represented by 3 to 12 indices. </br>
// MAGIC On the other hand at resolution 10 most of our geometries will be represented by 17 to 80 indices. </br>
// MAGIC We will select resolution 10. This gives a resonable trade off between fragmentation and size of representation.

// COMMAND ----------

randomized_neighbourhoods.select(
    col("borough"),
    col("geometry"),
    col("xmin"),
    col("xmax"),
    col("ymin"),
    col("ymax"),
    mosaic_explode(col("wkt"), lit(10))
  )
  .where(rand()*3 < 1) // take only 33% of data
  .write.format("delta").mode("overwrite").save("/Users/milos_colic/mosaic/demo/randomized_neighbourhoods_indexed_1M")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE randomized_neighbourhoods_indexed_1M
// MAGIC using delta
// MAGIC LOCATION "/Users/milos_colic/mosaic/demo/randomized_neighbourhoods_indexed_1M"

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE randomized_neighbourhoods_indexed_1M ZORDER BY (xmin, xmax, ymin, ymax, h3)

// COMMAND ----------

// MAGIC %md
// MAGIC These operations took about 45 minutes. </br>
// MAGIC For 3M geometries that generate about 630M indices and chips that is reasonable runtime. </br>
// MAGIC One worht noting is that our data has a heavy skew due the way we generated (purposefully). </br>
// MAGIC Partition runtime vary between 19 and 38 minutes for write stage, and between 1 and 3 minutes for optimize steps. </br>
// MAGIC Note: We have processed all 3M records sicne mosaic_explode does not yet support wholeStageCodegen (we are working on it), filters werent pushed down.

// COMMAND ----------

// MAGIC %md
// MAGIC We will finally aslo create and store the full 3M dataset. </br>
// MAGIC Note that the individual records will differe between the 2 runs due to random nature of the data.

// COMMAND ----------

randomized_neighbourhoods
  .select(
    col("borough"),
    col("geometry"),
    col("xmin"),
    col("xmax"),
    col("ymin"),
    col("ymax"),
    mosaic_explode(col("wkt"), lit(10))
  )
  .write.format("delta").mode("overwrite").save("/Users/milos_colic/mosaic/demo/randomized_neighbourhoods_indexed_3M")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE randomized_neighbourhoods_indexed_3M
// MAGIC using delta
// MAGIC LOCATION "/Users/milos_colic/mosaic/demo/randomized_neighbourhoods_indexed_3M"

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE randomized_neighbourhoods_indexed_3M ZORDER BY (xmin, xmax, ymin, ymax, h3)

// COMMAND ----------


