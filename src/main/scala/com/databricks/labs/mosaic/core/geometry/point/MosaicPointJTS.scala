package com.databricks.labs.mosaic.core.geometry.point

import com.databricks.labs.mosaic.core.geometry._
import com.databricks.labs.mosaic.core.geometry.linestring.MosaicLineString
import com.databricks.labs.mosaic.core.types.model.{GeometryTypeEnum, _}
import com.databricks.labs.mosaic.core.types.model.GeometryTypeEnum.POINT
import com.esotericsoftware.kryo.io.Input
import com.uber.h3core.util.GeoCoord
import org.locationtech.jts.geom._

import org.apache.spark.sql.catalyst.InternalRow

class MosaicPointJTS(point: Point) extends MosaicGeometryJTS(point) with MosaicPoint {

    override def geoCoord: GeoCoord = new GeoCoord(point.getY, point.getX)

    override def coord: Coordinate = new Coordinate(point.getX, point.getY)

    override def asSeq: Seq[Double] =
        if (point.getCoordinates.length == 2) {
            Seq(getX, getY)
        } else {
            Seq(getX, getY, getZ)
        }

    override def getX: Double = point.getX

    override def getY: Double = point.getY

    override def getZ: Double = point.getCoordinate.z

    override def toInternal: InternalGeometry = {
        val shell = Array(InternalCoord(point.getCoordinate))
        new InternalGeometry(POINT.id, getSpatialReference, Array(shell), Array(Array(Array())))
    }

    override def getBoundary: MosaicGeometry = {
        val geom = point.getBoundary
        geom.setSRID(point.getSRID)
        MosaicGeometryJTS(geom)
    }

    override def mapXY(f: (Double, Double) => (Double, Double)): MosaicGeometry = {
        val (x_, y_) = f(getX, getY)
        MosaicPointJTS(
          new Coordinate(x_, y_),
          point.getSRID
        )
    }

}

object MosaicPointJTS extends GeometryReader {

    def apply(geom: Geometry): MosaicPointJTS = new MosaicPointJTS(geom.asInstanceOf[Point])

    def apply(geoCoord: GeoCoord): MosaicPointJTS = {
        this.apply(new Coordinate(geoCoord.lng, geoCoord.lat), defaultSpatialReferenceId)
    }

    def apply(coord: Coordinate, srid: Int): MosaicPointJTS = {
        val gf = new GeometryFactory()
        val point = gf.createPoint(coord)
        point.setSRID(srid)
        new MosaicPointJTS(point)
    }

    override def fromInternal(row: InternalRow): MosaicGeometry = {
        val gf = new GeometryFactory()
        val internalGeom = InternalGeometry(row)
        val coordinate = internalGeom.boundaries.head.head
        val point = gf.createPoint(coordinate.toCoordinate)
        point.setSRID(internalGeom.srid)
        new MosaicPointJTS(point)
    }

    override def fromSeq[T <: MosaicGeometry](geomSeq: Seq[T], geomType: GeometryTypeEnum.Value = POINT): MosaicPointJTS = {
        val gf = new GeometryFactory()
        val spatialReference = geomSeq.head.getSpatialReference
        val newGeom = GeometryTypeEnum.fromString(geomSeq.head.getGeometryType) match {
            case POINT                         =>
                val extractedPoint = geomSeq.head.asInstanceOf[MosaicPoint]
                gf.createPoint(extractedPoint.coord)
            case other: GeometryTypeEnum.Value => throw new UnsupportedOperationException(
                  s"MosaicGeometry.fromSeq() cannot create ${geomType.toString} from ${other.toString} geometries."
                )
        }
        newGeom.setSRID(spatialReference)
        MosaicPointJTS(newGeom)
    }

    override def fromWKB(wkb: Array[Byte]): MosaicGeometry = MosaicGeometryJTS.fromWKB(wkb)

    override def fromWKT(wkt: String): MosaicGeometry = MosaicGeometryJTS.fromWKT(wkt)

    override def fromJSON(geoJson: String): MosaicGeometry = MosaicGeometryJTS.fromJSON(geoJson)

    override def fromHEX(hex: String): MosaicGeometry = MosaicGeometryJTS.fromHEX(hex)

    override def fromKryo(row: InternalRow): MosaicGeometry = {
        val kryoBytes = row.getBinary(1)
        val input = new Input(kryoBytes)
        MosaicGeometryJTS.kryo.readObject(input, classOf[MosaicPointJTS])
    }

}
