package com.databricks.mosaic.functions

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.functions.{lit, struct}

import com.databricks.mosaic.core.geometry.api.GeometryAPI
import com.databricks.mosaic.core.index.IndexSystem
import com.databricks.mosaic.expressions.constructors._
import com.databricks.mosaic.expressions.format._
import com.databricks.mosaic.expressions.geometry._
import com.databricks.mosaic.expressions.helper.TrySql
import com.databricks.mosaic.expressions.index._
import com.databricks.mosaic.functions.MosaicSQLExtensions.FunctionDescription



class MosaicContext(indexSystem: IndexSystem, geometryAPI: GeometryAPI) extends Serializable {

    import org.apache.spark.sql.adapters.{Column => ColumnAdapter}

    /**
      * Registers required parsers for SQL for Mosaic functionality.
      *
      * @param spark
      *   SparkSession to which the parsers are registered to.
      * @param database
      *   A database to which functions are added to. By default none is passed
      *   resulting in functions being registered in default database.
      */
    // noinspection ZeroIndexToHead
    // scalastyle:off line.size.limit
    def register(
        spark: SparkSession,
        database: Option[String] = None
    ): Unit = {
        val registry = spark.sessionState.functionRegistry
        val functionDescriptionMap = getFunctionDescriptions(database)
        functionDescriptionMap.foreach { case (_, (identifier, _, builder)) => registry.registerFunction(identifier, builder) }
    }

    // noinspection ZeroIndexToHead
    def getFunctionDescriptions(database: Option[String] = None): Map[String, FunctionDescription] = {
        def getExpressionInfo[T](expressionClass: Class[T], db: Option[String], expressionName: String) = db
            .map(dbName => new ExpressionInfo(expressionClass.getCanonicalName, dbName, expressionName))
            .getOrElse(new ExpressionInfo(expressionClass.getCanonicalName, expressionName))

        def getFunctionDescription[T](
            expressionClass: Class[T],
            db: Option[String],
            expressionName: String,
            builder: Seq[Expression] => Expression
        ): FunctionDescription = (
          FunctionIdentifier(expressionName, db),
          getExpressionInfo(expressionClass, db, expressionName),
          builder
        )

        Map(
          "as_hex" -> getFunctionDescription(classOf[AsHex], database, "as_hex", (exprs: Seq[Expression]) => AsHex(exprs(0))),
          "as_json" -> getFunctionDescription(classOf[AsJSON], database, "as_json", (exprs: Seq[Expression]) => AsJSON(exprs(0))),
          "st_point" -> getFunctionDescription(
            classOf[ST_Point],
            database,
            "st_point",
            (exprs: Seq[Expression]) => ST_Point(exprs(0), exprs(1))
          ),
          "st_makeline" -> getFunctionDescription(
            classOf[ST_MakeLine],
            database,
            "st_makeline",
            (exprs: Seq[Expression]) => ST_MakeLine(exprs(0))
          ),
          "st_polygon" -> getFunctionDescription(
            classOf[ST_MakePolygon],
            database,
            "st_polygon",
            (exprs: Seq[Expression]) => ST_MakePolygon(exprs(0))
          ),
          "st_polygon" -> getFunctionDescription(
            classOf[ST_MakePolygonWithHoles],
            database,
            "st_polygon",
            (exprs: Seq[Expression]) => ST_MakePolygonWithHoles(exprs(0), exprs(1))
          ),
          "index_geometry" -> getFunctionDescription(
            classOf[IndexGeometry],
            database,
            "index_geometry",
            (exprs: Seq[Expression]) => IndexGeometry(exprs(0), indexSystem.name, geometryAPI.name)
          ),
          "flatten_polygons" -> getFunctionDescription(
            classOf[FlattenPolygons],
            database,
            "flatten_polygons",
            (exprs: Seq[Expression]) => FlattenPolygons(exprs(0), geometryAPI.name)
          ),
          "st_xmax" -> getFunctionDescription(
            classOf[ST_MinMaxXYZ],
            database,
            "st_xmax",
            (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "X", "MAX")
          ),
          "st_xmin" -> getFunctionDescription(
            classOf[ST_MinMaxXYZ],
            database,
            "st_xmin",
            (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "X", "MIN")
          ),
          "st_ymax" -> getFunctionDescription(
            classOf[ST_MinMaxXYZ],
            database,
            "st_ymax",
            (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Y", "MAX")
          ),
          "st_ymin" -> getFunctionDescription(
            classOf[ST_MinMaxXYZ],
            database,
            "st_ymin",
            (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Y", "MIN")
          ),
          "st_zmax" -> getFunctionDescription(
            classOf[ST_MinMaxXYZ],
            database,
            "st_zmax",
            (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Z", "MAX")
          ),
          "st_zmin" -> getFunctionDescription(
            classOf[ST_MinMaxXYZ],
            database,
            "st_zmin",
            (exprs: Seq[Expression]) => ST_MinMaxXYZ(exprs(0), geometryAPI.name, "Z", "MIN")
          ),
          "st_isvalid" -> getFunctionDescription(
            classOf[ST_IsValid],
            database,
            "st_isvalid",
            (exprs: Seq[Expression]) => ST_IsValid(exprs(0), geometryAPI.name)
          ),
          "st_geometrytype" -> getFunctionDescription(
            classOf[ST_GeometryType],
            database,
            "st_geometrytype",
            (exprs: Seq[Expression]) => ST_GeometryType(exprs(0), geometryAPI.name)
          ),
          "st_area" -> getFunctionDescription(
            classOf[ST_Area],
            database,
            "st_area",
            (exprs: Seq[Expression]) => ST_Area(exprs(0), geometryAPI.name)
          ),
          "st_centroid2D" -> getFunctionDescription(
            classOf[ST_Centroid],
            database,
            "st_centroid2D",
            (exprs: Seq[Expression]) => ST_Centroid(exprs(0), geometryAPI.name)
          ),
          "st_centroid3D" -> getFunctionDescription(
            classOf[ST_Centroid],
            database,
            "st_centroid3D",
            (exprs: Seq[Expression]) => ST_Centroid(exprs(0), geometryAPI.name, 3)
          ),
          "st_geomfromwkt" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "st_geomfromwkt",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name)
          ),
          "st_geomfromwkb" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "st_geomfromwkb",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name)
          ),
          "st_geomfromgeojson" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "st_geomfromgeojson",
            (exprs: Seq[Expression]) => ConvertTo(AsJSON(exprs(0)), "coords", geometryAPI.name)
          ),
          "convert_to_hex" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "convert_to_hex",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "hex", geometryAPI.name)
          ),
          "convert_to_wkt" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "convert_to_wkt",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name)
          ),
          "convert_to_wkb" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "convert_to_wkb",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name)
          ),
          "convert_to_coords" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "convert_to_coords",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "coords", geometryAPI.name)
          ),
          "convert_to_geojson" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "convert_to_geojson",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "geojson", geometryAPI.name)
          ),
          "st_aswkt" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "st_aswkt",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name)
          ),
          "st_astext" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "st_astext",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkt", geometryAPI.name)
          ),
          "st_aswkb" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "st_aswkb",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name)
          ),
          "st_asbinary" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "st_asbinary",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "wkb", geometryAPI.name)
          ),
          "st_ashex" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "st_ashex",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "hex", geometryAPI.name)
          ),
          "st_asgeojson" -> getFunctionDescription(
            classOf[ConvertTo],
            database,
            "st_asgeojson",
            (exprs: Seq[Expression]) => ConvertTo(exprs(0), "geojson", geometryAPI.name)
          ),
          "st_length" -> getFunctionDescription(
            classOf[ST_Length],
            database,
            "st_length",
            (exprs: Seq[Expression]) => ST_Length(exprs(0), geometryAPI.name)
          ),
          "st_perimeter" -> getFunctionDescription(
            classOf[ST_Length],
            database,
            "st_perimeter",
            (exprs: Seq[Expression]) => ST_Length(exprs(0), geometryAPI.name)
          ),
          "st_distance" -> getFunctionDescription(
            classOf[ST_Distance],
            database,
            "st_distance",
            (exprs: Seq[Expression]) => ST_Distance(exprs(0), exprs(1), geometryAPI.name)
          ),
          "st_contains" -> getFunctionDescription(
            classOf[ST_Contains],
            database,
            "st_contains",
            (exprs: Seq[Expression]) => ST_Contains(exprs(0), exprs(1), geometryAPI.name)
          ),
          "st_translate" -> getFunctionDescription(
            classOf[ST_Translate],
            database,
            "st_translate",
            (exprs: Seq[Expression]) => ST_Translate(exprs(0), exprs(1), exprs(2), geometryAPI.name)
          ),
          "st_scale" -> getFunctionDescription(
            classOf[ST_Scale],
            database,
            "st_scale",
            (exprs: Seq[Expression]) => ST_Scale(exprs(0), exprs(1), exprs(2), geometryAPI.name)
          ),
          "st_rotate" -> getFunctionDescription(
            classOf[ST_Rotate],
            database,
            "st_rotate",
            (exprs: Seq[Expression]) => ST_Rotate(exprs(0), exprs(1), geometryAPI.name)
          ),
          "st_convexhull" -> getFunctionDescription(
            classOf[ST_ConvexHull],
            database,
            "st_convexhull",
            (exprs: Seq[Expression]) => ST_ConvexHull(exprs(0), geometryAPI.name)
          ),
          "mosaic_explode" -> getFunctionDescription(
            classOf[MosaicExplode],
            database,
            "mosaic_explode",
            (exprs: Seq[Expression]) =>
                MosaicExplode(struct(ColumnAdapter(exprs(0)), ColumnAdapter(exprs(1))).expr, indexSystem.name, geometryAPI.name)
          ),
          "mosaicfill" -> getFunctionDescription(
            classOf[MosaicFill],
            database,
            "mosaicfill",
            (exprs: Seq[Expression]) => MosaicFill(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
          ),
          "point_index" -> getFunctionDescription(
            classOf[PointIndex],
            database,
            "point_index",
            (exprs: Seq[Expression]) => PointIndex(exprs(0), exprs(1), exprs(2), indexSystem.name)
          ),
          "polyfill" -> getFunctionDescription(
            classOf[Polyfill],
            database,
            "polyfill",
            (exprs: Seq[Expression]) => Polyfill(exprs(0), exprs(1), indexSystem.name, geometryAPI.name)
          ),
          "st_dump" -> getFunctionDescription(
            classOf[Polyfill],
            database,
            "st_dump",
            (exprs: Seq[Expression]) => FlattenPolygons(exprs(0), geometryAPI.name)
          ),
          "try_sql" -> getFunctionDescription(
            classOf[Polyfill],
            database,
            "try_sql",
            (exprs: Seq[Expression]) => TrySql(exprs(0))
          )
        )
    }

    def getGeometryAPI: GeometryAPI = this.geometryAPI

    def getIndexSystem: IndexSystem = this.indexSystem

    // scalastyle:off object.name
    object functions extends Serializable {

        /** IndexSystem and GeometryAPI Agnostic methods */
        def as_hex(inGeom: Column): Column = ColumnAdapter(AsHex(inGeom.expr))
        def as_json(inGeom: Column): Column = ColumnAdapter(AsJSON(inGeom.expr))
        def st_point(xVal: Column, yVal: Column): Column = ColumnAdapter(ST_Point(xVal.expr, yVal.expr))
        def st_makeline(points: Column): Column = ColumnAdapter(ST_MakeLine(points.expr))
        def st_makepolygon(boundaryRing: Column): Column = ColumnAdapter(ST_MakePolygon(boundaryRing.expr))
        def st_makepolygon(boundaryRing: Column, holeRingArray: Column): Column =
            ColumnAdapter(ST_MakePolygonWithHoles(boundaryRing.expr, holeRingArray.expr))

        /** GeometryAPI Specific */
        def flatten_polygons(geom: Column): Column = ColumnAdapter(FlattenPolygons(geom.expr, geometryAPI.name))
        def st_xmax(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "X", "MAX"))
        def st_xmin(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "X", "MIN"))
        def st_ymax(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Y", "MAX"))
        def st_ymin(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Y", "MIN"))
        def st_zmax(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Z", "MAX"))
        def st_zmin(geom: Column): Column = ColumnAdapter(ST_MinMaxXYZ(geom.expr, geometryAPI.name, "Z", "MIN"))
        def st_isvalid(geom: Column): Column = ColumnAdapter(ST_IsValid(geom.expr, geometryAPI.name))
        def st_geometrytype(geom: Column): Column = ColumnAdapter(ST_GeometryType(geom.expr, geometryAPI.name))
        def st_area(geom: Column): Column = ColumnAdapter(ST_Area(geom.expr, geometryAPI.name))
        def st_centroid2D(geom: Column): Column = ColumnAdapter(ST_Centroid(geom.expr, geometryAPI.name))
        def st_centroid3D(geom: Column): Column = ColumnAdapter(ST_Centroid(geom.expr, geometryAPI.name, 3))
        def convert_to(inGeom: Column, outDataType: String): Column = ColumnAdapter(ConvertTo(inGeom.expr, outDataType, geometryAPI.name))
        def st_geomfromwkt(inGeom: Column): Column = ColumnAdapter(ConvertTo(inGeom.expr, "coords", geometryAPI.name))
        def st_geomfromwkb(inGeom: Column): Column = ColumnAdapter(ConvertTo(inGeom.expr, "coords", geometryAPI.name))
        def st_geomfromgeojson(inGeom: Column): Column = ColumnAdapter(ConvertTo(AsJSON(inGeom.expr), "coords", geometryAPI.name))
        def st_aswkt(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkt", geometryAPI.name))
        def st_astext(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkt", geometryAPI.name))
        def st_aswkb(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkb", geometryAPI.name))
        def st_asbinary(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "wkb", geometryAPI.name))
        def st_asgeojson(geom: Column): Column = ColumnAdapter(ConvertTo(geom.expr, "geojson", geometryAPI.name))
        def st_dump(geom: Column): Column = ColumnAdapter(FlattenPolygons(geom.expr, geometryAPI.name))
        def st_length(geom: Column): Column = ColumnAdapter(ST_Length(geom.expr, geometryAPI.name))
        def st_perimeter(geom: Column): Column = ColumnAdapter(ST_Length(geom.expr, geometryAPI.name))
        def st_distance(geom1: Column, geom2: Column): Column = ColumnAdapter(ST_Distance(geom1.expr, geom2.expr, geometryAPI.name))
        def st_contains(geom1: Column, geom2: Column): Column = ColumnAdapter(ST_Contains(geom1.expr, geom2.expr, geometryAPI.name))
        def st_translate(geom1: Column, xd: Column, yd: Column): Column =
            ColumnAdapter(ST_Translate(geom1.expr, xd.expr, yd.expr, geometryAPI.name))
        def st_scale(geom1: Column, xd: Column, yd: Column): Column =
            ColumnAdapter(ST_Scale(geom1.expr, xd.expr, yd.expr, geometryAPI.name))
        def st_rotate(geom1: Column, td: Column): Column = ColumnAdapter(ST_Rotate(geom1.expr, td.expr, geometryAPI.name))
        def st_convexhull(geom: Column): Column = ColumnAdapter(ST_ConvexHull(geom.expr, geometryAPI.name))

        /** IndexSystem Specific */

        /** IndexSystem and GeometryAPI Specific methods */
        def mosaic_explode(geom: Column, resolution: Column): Column =
            ColumnAdapter(MosaicExplode(struct(geom, resolution).expr, indexSystem.name, geometryAPI.name))
        def mosaic_explode(geom: Column, resolution: Int): Column =
            ColumnAdapter(MosaicExplode(struct(geom, lit(resolution)).expr, indexSystem.name, geometryAPI.name))
        def mosaicfill(geom: Column, resolution: Column): Column =
            ColumnAdapter(MosaicFill(geom.expr, resolution.expr, indexSystem.name, geometryAPI.name))
        def mosaicfill(geom: Column, resolution: Int): Column =
            ColumnAdapter(MosaicFill(geom.expr, lit(resolution).expr, indexSystem.name, geometryAPI.name))
        def point_index(lat: Column, lng: Column, resolution: Column): Column =
            ColumnAdapter(PointIndex(lat.expr, lng.expr, resolution.expr, indexSystem.name))
        def point_index(lat: Column, lng: Column, resolution: Int): Column =
            ColumnAdapter(PointIndex(lat.expr, lng.expr, lit(resolution).expr, indexSystem.name))
        def polyfill(geom: Column, resolution: Column): Column =
            ColumnAdapter(Polyfill(geom.expr, resolution.expr, indexSystem.name, geometryAPI.name))
        def polyfill(geom: Column, resolution: Int): Column =
            ColumnAdapter(Polyfill(geom.expr, lit(resolution).expr, indexSystem.name, geometryAPI.name))
        def index_geometry(indexID: Column): Column = ColumnAdapter(IndexGeometry(indexID.expr, indexSystem.name, geometryAPI.name))

        // Not specific to Mosaic
        def try_sql(inCol: Column): Column = ColumnAdapter(TrySql(inCol.expr))

    }

}
// scalastyle:on object.name
// scalastyle:on line.size.limit

object MosaicContext {

    private var instance: Option[MosaicContext] = None

    def build(indexSystem: IndexSystem, geometryAPI: GeometryAPI): MosaicContext = {
        instance = Some(new MosaicContext(indexSystem, geometryAPI))
        context
    }

    def context: MosaicContext =
        instance match {
            case Some(context) => context
            case None          => throw new IllegalStateException("MosaicContext was not built.")
        }

    def geometryAPI: GeometryAPI = context.getGeometryAPI

    def indexSystem: IndexSystem = context.getIndexSystem

}
