# Databricks notebook source
# DBTITLE 1,Configuration
mosaic_kepler_config = {
  "version": "v1",
  "config": {
    "visState": {
      "filters": [],
      "layers": [
        {
          "id": "nxtu8fe",
          "type": "hexagonId",
          "config": {
            "dataId": "hexes_res_10",
            "label": "id",
            "color": [
              18,
              147,
              154
            ],
            "columns": {
              "hex_id": "id"
            },
            "isVisible": True,
            "visConfig": {
              "opacity": 0.59,
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "coverage": 1,
              "enable3d": False,
              "sizeRange": [
                0,
                500
              ],
              "coverageRange": [
                0,
                1
              ],
              "elevationScale": 5
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": None,
            "colorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear",
            "coverageField": None,
            "coverageScale": "linear"
          }
        },
        {
          "id": "eeksejt",
          "type": "geojson",
          "config": {
            "dataId": "raw_polygon",
            "label": "raw_polygon",
            "color": [
              221,
              178,
              124
            ],
            "columns": {
              "geojson": "wkt_polygons"
            },
            "isVisible": True,
            "visConfig": {
              "opacity": 0.1,
              "strokeOpacity": 0.8,
              "thickness": 0.5,
              "strokeColor": [
                136,
                87,
                44
              ],
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radius": 10,
              "sizeRange": [
                0,
                10
              ],
              "radiusRange": [
                0,
                50
              ],
              "heightRange": [
                0,
                500
              ],
              "elevationScale": 5,
              "stroked": True,
              "filled": True,
              "enable3d": False,
              "wireframe": False
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": None,
            "colorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "heightField": None,
            "heightScale": "linear",
            "radiusField": None,
            "radiusScale": "linear"
          }
        }
      ],
      "interactionConfig": {
        "tooltip": {
          "fieldsToShow": {
            "hexes_res_10": [
              {
                "name": "id",
                "format": None
              }
            ],
            "raw_polygon": [
              {
                "name": "id",
                "format": None
              }
            ]
          },
          "compareMode": False,
          "compareType": "absolute",
          "enabled": True
        },
        "brush": {
          "size": 2.2,
          "enabled": False
        },
        "geocoder": {
          "enabled": False
        },
        "coordinate": {
          "enabled": False
        }
      },
      "layerBlending": "normal",
      "splitMaps": [],
      "animationConfig": {
        "currentTime": None,
        "speed": 1
      }
    },
    "mapState": {
      "bearing": 0,
      "dragRotate": False,
      "latitude": 40.766233510042554,
      "longitude": -73.9219485077532,
      "pitch": 0,
      "zoom": 13.846148233536928,
      "isSplit": False
    },
    "mapStyle": {
      "styleType": "dark",
      "topLayerGroups": {},
      "visibleLayerGroups": {
        "label": True,
        "road": True,
        "border": False,
        "building": True,
        "water": True,
        "land": True,
        "3d building": False
      },
      "threeDBuildingColor": [
        9.665468314072013,
        17.18305478057247,
        31.1442867897876
      ],
      "mapStyles": {}
    }
  }
}

# COMMAND ----------

import sys
import pandas as pd
from IPython.core.magic import Magics, magics_class, cell_magic
from pyspark.sql import functions as F
import keplergl
from keplergl import KeplerGl
import h3

@magics_class
class MosaicKepler(Magics):
  
  def displayKepler(self, map_instance, height, width):
    displayHTML(map_instance._repr_html_().decode("utf-8").replace(
        ".height||400", f".height||{height}"
    ))

  @cell_magic
  def mosaic_kepler(self, *args):
    "Replace current line with new output"
    
    inputs = [i for i in " ".join(list(args)).replace("\n", " ").replace("\"", "").split(" ") if len(i) > 0]
        
    if len(inputs) != 3 and len(inputs) != 4:
      raise Exception("Mosaic Kepler magic requires table name, feature column and feature type all to be provided. Limit is optional (default 1000).")
    
    table_name = inputs[0]
    feature_name = inputs[1]
    feature_type = inputs[2]
    limitCtn = 1000
    if len(inputs) == 4:
      limitCtn = int(inputs[3])
    
    data = spark.read.table(table_name)
    feature_col_dt = [dt for dt in data.dtypes if dt[0] == feature_name][0]
    
    if feature_type == "h3":
      if feature_col_dt[1] == "bigint":
        data = data.withColumn(feature_name, F.lower(F.conv(F.col(feature_name), 10, 16)))
    elif feature_type == "geometry":
      raise Exception(f"Usupported geometry type: {feature_type}.")
    else:
      raise Exception(f"Usupported geometry type: {feature_type}.")
    
    toRender = data.limit(limitCtn).cache()
    pandasData = toRender.limit(limitCtn).toPandas()    
    
    
    centroid = h3.h3_to_geo(pandasData[feature_name][0])
    mosaic_kepler_config["config"]["mapState"]["latitude"] = centroid[0] # set to centroid of a geom
    mosaic_kepler_config["config"]["mapState"]["longitude"] = centroid[1] # se to centrodi of a geom

    
    m1 = KeplerGl(config=mosaic_kepler_config)
    m1.add_data(data=pandasData, name = table_name)
    
    logo_html = "<img src='/files/milos_colic/mosaic_logo.png' height='20px'>"

    displayHTML(logo_html)
    self.displayKepler(m1, 800, 1200)

ip = get_ipython()
print("Adding Magic to support %python %%mosaic_kepler")
ip.register_magics(MosaicKepler)
