package com.databricks.labs.mosaic.expressions.geometry

import scala.collection.TraversableOnce

import com.databricks.labs.mosaic.core.geometry.api.GeometryAPI
import com.databricks.labs.mosaic.core.types._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ExpressionInfo, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class FlattenPolygons(pair: Expression, geometryAPIName: String)
    extends UnaryExpression
      with CollectionGenerator
      with CodegenFallback {

    /** Fixed definitions. */
    override val inline: Boolean = false

    override def collectionType: DataType = child.dataType

    override def position: Boolean = false

    /**
      * @see
      *   [[FlattenPolygons()]] companion object for implementations.
      */
    override def checkInputDataTypes(): TypeCheckResult = FlattenPolygons.checkInputDataTypesImpl(child)

    override def elementSchema: StructType = FlattenPolygons.elementSchemaImpl(child)

    override def child: Expression = pair

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = FlattenPolygons.evalImpl(input, child, geometryAPIName)

    override def makeCopy(newArgs: Array[AnyRef]): Expression = FlattenPolygons.makeCopyImpl(newArgs, this, geometryAPIName)

    override protected def withNewChildInternal(newChild: Expression): Expression = copy(pair = newChild)

}

object FlattenPolygons {

    /**
      * Flattens an input into a collection of outputs. Each output instance
      * should be wrapped into an [[InternalRow]] wrapper. For the generator
      * expression [[evalImpl()]] call requires that input is evaluated before
      * the evaluation of this expression can occur.
      *
      * @param input
      *   An instance of a row before the child expression has been evaluated.
      * @return
      *   A collection of [[InternalRow]] instances. This collection has to
      *   implement [[TraversableOnce]] API.
      */
    def evalImpl(input: InternalRow, child: Expression, geometryAPIName: String): TraversableOnce[InternalRow] = {
        val geometryAPI = GeometryAPI(geometryAPIName)
        val geometry = geometryAPI.geometry(input, child.dataType)
        val output = geometry.flatten

        child.dataType match {
            case _: BinaryType           => // WKB case
                output.map(g => InternalRow.fromSeq(Seq(g.toWKB)))
            case _: StringType           => // WTK case
                output.map(g => InternalRow.fromSeq(Seq(UTF8String.fromString(g.toWKT))))
            case _: HexType              => // HEX case
                output.map(g =>
                    InternalRow.fromSeq(
                      Seq(
                        InternalRow.fromSeq(Seq(UTF8String.fromString(g.toHEX)))
                      )
                    )
                )
            case _: JSONType             => // GEOJSON case
                output.map(g =>
                    InternalRow.fromSeq(
                      Seq(
                        InternalRow.fromSeq(Seq(UTF8String.fromString(g.toJSON)))
                      )
                    )
                )
            case _: InternalGeometryType => // COORDS case
                output.map(g => InternalRow.fromSeq(Seq(g.toInternal.serialize)))
        }
    }

    /**
      * [[FlattenPolygons]] expression can only be called on supported data
      * types. The supported data types are [[BinaryType]] for WKB encoding,
      * [[StringType]] for WKT encoding, [[HexType]] ([[StringType]] wrapper)
      * for HEX encoding and [[InternalGeometryType]] for primitive types
      * encoding via [[ArrayType]].
      *
      * @return
      *   An instance of [[TypeCheckResult]] indicating success or a failure.
      */
    def checkInputDataTypesImpl(child: Expression): TypeCheckResult =
        child.dataType match {
            case _: BinaryType           => TypeCheckResult.TypeCheckSuccess
            case _: StringType           => TypeCheckResult.TypeCheckSuccess
            case _: HexType              => TypeCheckResult.TypeCheckSuccess
            case _: InternalGeometryType => TypeCheckResult.TypeCheckSuccess
            case _                       => TypeCheckResult.TypeCheckFailure(
                  "input to function explode should be array or map type, " +
                      s"not ${child.dataType.catalogString}"
                )
        }

    /**
      * [[FlattenPolygons]] is a generator expression. All generator expressions
      * require the element schema to be provided. Since we are flattening the
      * geometries the element type is the same type of the input data.
      *
      * @see
      *   [[CollectionGenerator]] for the API of generator expressions.
      * @return
      *   The schema of the child element. Has to be provided as a
      *   [[StructType]].
      */
    def elementSchemaImpl(child: Expression): StructType =
        child.dataType match {
            case _: BinaryType           => StructType(Seq(StructField("element", BinaryType)))
            case _: StringType           => StructType(Seq(StructField("element", StringType)))
            case _: HexType              => StructType(Seq(StructField("element", HexType)))
            case _: InternalGeometryType => StructType(Seq(StructField("element", InternalGeometryType)))
        }

    def makeCopyImpl(newArgs: Array[AnyRef], instance: Expression, geometryAPIName: String): Expression = {
        val asArray = newArgs.take(1).map(_.asInstanceOf[Expression])
        val res = FlattenPolygons(asArray(0), geometryAPIName)
        res.copyTagsFrom(instance)
        res
    }

    /** Entry to use in the function registry. */
    def registryExpressionInfo(db: Option[String]): ExpressionInfo =
        new ExpressionInfo(
          classOf[FlattenPolygons].getCanonicalName,
          db.orNull,
          "flatten_polygons",
          """
            |    _FUNC_(geometry) - The geometry instance can contain both Polygons and MultiPolygons.
            |    The flattened representation will only contain Polygons.
            |    MultiPolygon rows will be exploded into Polygon rows
            """.stripMargin,
          "",
          """
            |    Examples:
            |      > SELECT _FUNC_(a);
            |        Polygon ((...))
            |        Polygon ((...))
            |        ...
            |        Polygon ((...))
            |  """.stripMargin,
          "",
          "generator_funcs",
          "1.0",
          "",
          "built-in"
        )

}
