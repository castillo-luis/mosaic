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

// DBTITLE 1,Enable Mosaic
// MAGIC %run "./../setup/EnableMosaic"

// COMMAND ----------

// DBTITLE 1,Spark SQL functions import
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Paths to data location
val username = "milos.colic" 
val silver_data_location = s"Users/${username}/geospatial/workshop/data/silver"

// COMMAND ----------

// MAGIC %md
// MAGIC Mosaic comes with an automatic pretty print display function. </br>
// MAGIC displayMosaic will detect if a keyword is present in the column name and will convert the column to WKT representation. </br>
// MAGIC For all other colums displayMosaic will work in the same way display function works. </br>
// MAGIC displayMosaic only affects columns at the top level of the schema.

// COMMAND ----------

val polygons1 = spark.read.format("delta").load(s"/${silver_data_location}/h3/neighbourhoods/random/dataset_1_decomposed")
display(polygons1)

// COMMAND ----------

displayMosaic(polygons1)

// COMMAND ----------

// MAGIC %md
// MAGIC Mosaic comes with a built in support for H3 on SQL. </br>
// MAGIC Support for S2 and BNG will be added in the future versions. </br>
// MAGIC Index system instance is controlled via available MosaicContext instance. </br>
// MAGIC Note: We do not advise mixing of index systems and/or geometry API providers.
// MAGIC Please stick to one choice within a logical step of data processing.
// MAGIC You can switch between systems in separate notebook.

// COMMAND ----------

mosaicContext.getIndexSystem.name

// COMMAND ----------

displayMosaic(
  polygons1.select(
    polyfill(col("wkb_polygon"), 10).alias("x")
  )
)

// COMMAND ----------

// MAGIC %md
// MAGIC Mosaic comes with many operations supporting the ST_x syntax. </br>
// MAGIC Mosaic also comes with operations supporting representation of geometries as described in this [blog](https://databricks.com/blog/2021/10/11/efficient-point-in-polygon-joins-via-pyspark-and-bng-geospatial-indexing.html). </br>
// MAGIC At the moment indexing system used is H3.

// COMMAND ----------

displayMosaic(
  MosaicFrame(polygons1.select(mosaic_explode(col("wkb_polygon"), lit(10))).withColumn("wkb_chip", col("wkb")))
)

// COMMAND ----------

polygons1.createOrReplaceTempView("polygons1")

// COMMAND ----------

// MAGIC %md
// MAGIC Mosaic supports scala, python and SQL syntax. </br>
// MAGIC All the methods are registered with the spark session function registry and can also be accessed via expr(c1) calls. </br>
// MAGIC mosaicDisplay is not available in SQL.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select st_astext(wkb), * from (
// MAGIC   select mosaic_explode(wkb_polygon, 10)
// MAGIC   from polygons1
// MAGIC )

// COMMAND ----------

// MAGIC %md
// MAGIC Selecting a correct resolution for data representation isnt a trivial task. </br>
// MAGIC That is why Mosaic comes with a geometry analyzer method available through MosaicFrame API. </br>
// MAGIC Simply wrap a dataframe into a MosaicFrame and mark which column is to be used as a feature column. </br>
// MAGIC We will generate a set of metrics describing the ratio between feature area and index area. </br>
// MAGIC That way you can understand the row explosion rate if you represent your data as a mosaic.

// COMMAND ----------

import com.databricks.mosaic.sql.MosaicFrame

val metrics = MosaicFrame(polygons1)
  .setGeom("wkb_polygon")
  .getResolutionMetrics(5, 300)

// COMMAND ----------

display(metrics)

// COMMAND ----------

// MAGIC %md
// MAGIC Mosaic comes with bindings for Python for all the spark fucntions that are supported in scala and sql. </br>
// MAGIC Mosaic also comes with a magic for displaying H3 indices in KeplerGL.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.sql import functions as F
// MAGIC 
// MAGIC polygons1 = spark.read.table("polygons1")
// MAGIC 
// MAGIC displayMosaic(
// MAGIC   polygons1.select(mosaic_explode(F.col("wkb_polygon"), F.lit(10))).withColumn("wkb_chip", F.col("wkb"))
// MAGIC )

// COMMAND ----------

val kepler_df = polygons1
  .select(mosaic_explode(col("wkb_polygon"), lit(10)))
  .withColumn("wkt_chip", st_astext(col("wkb")))
  .withColumn("h3_id", col("h3"))
  .select("wkt_chip", "h3_id")

kepler_df.createOrReplaceTempView("kepler_df")

// COMMAND ----------

displayMosaic(
    polygons1.withColumn("numPoints", convert_to(col("wkb_polygon")))
)

// COMMAND ----------

// MAGIC %python
// MAGIC %%mosaic_kepler 
// MAGIC "kepler_df" "h3_id" "h3"

// COMMAND ----------

// MAGIC %md
// MAGIC Mosaic comes with OGC and JTS bindings for geometries. </br>
// MAGIC You can use Mosaic geometries with unified API as transient objects. </br>
// MAGIC Dataset api allows us to perform complex operations directly on geometries. </br>
// MAGIC This can be very useful when operations are not available in mosaic.functions.

// COMMAND ----------

import com.databricks.mosaic.core.geometry.MosaicGeometryOGC
import com.databricks.mosaic.core.geometry.multilinestring.MosaicMultiLineStringOGC
val ss = spark
import ss.implicits._

displayMosaic(
  polygons1.select("wkb_polygon").as[Array[Byte]].map(
    wkb => {
      val geom = MosaicGeometryOGC.fromWKB(wkb)
      val multiLineString = geom.boundary.asInstanceOf[MosaicMultiLineStringOGC]
      val coords = multiLineString.toInternal
      val nVertices = coords.boundaries.map(_.size).sum + coords.holes.map(_.map(_.size).sum).sum
      (multiLineString.toWKT, multiLineString.getGeometryType, nVertices)
    }
  ).toDF
)

// COMMAND ----------


