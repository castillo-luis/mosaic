// Databricks notebook source
import com.databricks.mosaic.functions.MosaicContext
import com.databricks.mosaic.OGC
import com.databricks.mosaic.H3
val mosaicContext: MosaicContext = MosaicContext.build(H3, OGC)
import mosaicContext.functions._

// COMMAND ----------

// MAGIC %run "./GeneratorsHotfix"

// COMMAND ----------

import com.databricks.mosaic.patch.MosaicPatch
import com.databricks.mosaic.OGC
import com.databricks.mosaic.H3
val mosaicPatch = MosaicPatch(H3, OGC)
import mosaicPatch.functions._

// COMMAND ----------

mosaicContext.register(spark)
mosaicPatch.register(spark)

// COMMAND ----------

// MAGIC %run "./PythonBindings"

// COMMAND ----------

import org.apache.spark.sql._
import com.databricks.mosaic.sql.MosaicFrame

def displayMosaic(df: DataFrame) = {
  val mosaicFrame = MosaicFrame(df)  
  display(mosaicFrame.prettified)
}

def displayMosaic(mf: MosaicFrame) = {
  display(mf.prettified)
}

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql import DataFrame
// MAGIC from pyspark.sql import SQLContext
// MAGIC 
// MAGIC PrettifierObjectClass = getattr(sc._jvm.com.databricks.mosaic.sql, "Prettifier$")
// MAGIC PrettifierObject = getattr(PrettifierObjectClass, "MODULE$")
// MAGIC sqlCtx = SQLContext(spark.sparkContext)
// MAGIC 
// MAGIC def displayMosaic(df):
// MAGIC   display(
// MAGIC     DataFrame(
// MAGIC       PrettifierObject.prettified(df._jdf),
// MAGIC       sqlCtx
// MAGIC     )
// MAGIC   )
