// Databricks notebook source
// MAGIC %run "./../setup/EnableMosaic"

// COMMAND ----------

displayHTML("""
<svg height="210" width="500">
  <polygon points="100,10 40,198 190,78 10,78 160,198" style="fill:lime;stroke:purple;stroke-width:2;fill-rule:nonzero;"/>
  Sorry, your browser does not support inline SVG.
</svg>

""")

// COMMAND ----------

import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.core.index.H3IndexSystem
import com.databricks.mosaic.core.geometry.api.GeometryAPI.OGC

val mc = com.databricks.mosaic.functions.MosaicContext.build(H3IndexSystem, OGC)

// COMMAND ----------

import mc.functions._

// COMMAND ----------

// MAGIC %python
// MAGIC import os
// MAGIC 
// MAGIC os.system("wget http://d3js.org/d3.v3.min.js -O /dbfs/FileStore/milos_colic/d3.v3.min.js d3.v3.min.js")

// COMMAND ----------

// MAGIC %fs ls /FileStore/milos_colic/

// COMMAND ----------

displayHTML("""
<html>
    <head>
      <script src="/files/milos_colic/d3.v3.min.js" charset="utf-8"></script>
    </head>
    <body>
      <div id="example">
        <svg width="100" height="100">
          <circle id = "myCircle" cx="50" cy="50" r="30" ></circle>
        </svg>
      </div>
      <script type="text/javascript">
        var circleDemo = d3.select("#myCircle");
          circleDemo.attr("r", 40);
          circleDemo.style("stroke", "black");
          circleDemo.style("fill", "orange");    
      </script>
    </body>
  </html>
""")

// COMMAND ----------

val neighbourhoodIndices = spark.read.table("randomized_neighbourhoods_indexed_3M").drop("geometry")

// COMMAND ----------

import scala.reflect.runtime.universe

import scala.reflect.runtime.universe
val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
val module = runtimeMirror.staticModule("org.apache.spark.sql.catalyst.trees.TreePattern")
val obj = runtimeMirror.reflectModule(module).instance
val generator = obj.getClass.getDeclaredMethods.toSeq.find(_.getName.contains("GENERATOR"))
val valueMethod = obj.getClass.getDeclaredMethod(generator.map(_.getName).getOrElse(null))

// COMMAND ----------

valueMethod.invoke(obj)

// COMMAND ----------

x.getClass.getDeclaredMethods.toSeq.map(m => (m, m.toString)).find(_._2.contains("GENERATOR()")).map(
  g => x.getClass.getDeclaredMethod(g._1.getName)
).getOrElse(null)

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  neighbourhoodIndices.select(flatten_polygons(col("wkb"))).where(col("wkb").isNotNull).limit(100)
  )

// COMMAND ----------

displayMosaic(neighbourhoodIndices)

// COMMAND ----------

import org.apache.spark.sql.functions._

val toDraw = neighbourhoodIndices.select(
  when(
    col("is_core"), 
    index_geometry(col("h3"))
  ).otherwise(
    col("wkb")
  ).alias("polygon")
)

// COMMAND ----------

import com.databricks.mosaic.core.geometry.MosaicGeometryOGC
import com.databricks.mosaic.core.geometry.multilinestring.MosaicMultiLineStringOGC
import scala.util.Try
val ss = spark
import ss.implicits._

val withSvg = toDraw.select("polygon").as[Array[Byte]].map(
  wkb => {
    Try {
      val geom = MosaicGeometryOGC.fromWKB(wkb)
      val boundaries = geom.toInternal.boundaries
      val holes = geom.toInternal.holes
      boundaries.zip(holes)
      s"""|
          |<svg height="210" width="500">
          |  <polygon points="100,10 40,198 190,78 10,78 160,198" style="fill:lime;stroke:purple;stroke-width:2;fill-rule:nonzero;"/>
          |      Sorry, your browser does not support inline SVG.
          |</svg>
          |
          |""".stripMargin
    }.toOption
    
  }
).toDF


// COMMAND ----------

displayMosaic(withSvg.withColumn("a", lit("a")))

// COMMAND ----------

displayMosaic(toDraw)

// COMMAND ----------


