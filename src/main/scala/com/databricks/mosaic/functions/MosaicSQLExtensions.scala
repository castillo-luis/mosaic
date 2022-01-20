package com.databricks.mosaic.functions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

import com.databricks.mosaic.core.geometry.api.GeometryAPI.{JTS, OGC}
import com.databricks.mosaic.core.index.H3IndexSystem

object MosaicSQLExtensions extends SparkSessionExtensions with Logging {

    val mosaicContext_H3_OGC: MosaicContext = MosaicContext.build(H3IndexSystem, OGC)
    val mosaicContext_H3_JTS: MosaicContext = MosaicContext.build(H3IndexSystem, JTS)

    // noinspection DuplicatedCode
    class H3_OGC extends (SparkSessionExtensions => Unit) {

        // noinspection ScalaStyle
        override def apply(ext: SparkSessionExtensions): Unit = {
            logInfo("Registering Mosaic SQL Extensions.")
            val functionDescriptionMap: Map[String, (FunctionIdentifier, ExpressionInfo, FunctionBuilder)] =
                mosaicContext_H3_OGC.getFunctionDescriptions()
            functionDescriptionMap.foreach { case (_, functionDescription) => ext.injectFunction(functionDescription) }
            ext.injectCheckRule(spark => {
                mosaicContext_H3_OGC.register(spark)
                _ => ()
            })
        }

    }

    // noinspection DuplicatedCode
    class H3_JTS extends (SparkSessionExtensions => Unit) {

        // noinspection ScalaStyle
        override def apply(ext: SparkSessionExtensions): Unit = {
            logInfo("Registering Mosaic SQL Extensions.")
//            val functionDescriptionMap: Map[String, (FunctionIdentifier, ExpressionInfo, FunctionBuilder)] =
//                mosaicContext_H3_JTS.getFunctionDescriptions()
//            functionDescriptionMap.foreach { case (_, functionDescription) => ext.injectFunction(functionDescription) }
            ext.injectCheckRule(spark => {
                mosaicContext_H3_OGC.register(spark)
                _ => ()
            })
        }

    }

}
