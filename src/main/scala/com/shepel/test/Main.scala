package com.shepel.test

import com.shepel.test.CoordinatesUtils.calculateDistance

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, to_timestamp}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

import java.util

object Main {

  private val INPUT_PATH: String = "inputPath"
  private val INPUT_POI_FILE: String = "inputPoiFile"
  private val DAYS_TO_FILTER: String = "daysToFilter"
  private val OUTPUT_PATH: String = "outputPath"


  def main(args: Array[String]) {

    val optMap = args.map(s => {
      val split = s.split("=")
      (split(0), split(1))
    }).toMap


    val inputPath = if (optMap.contains(INPUT_PATH)) optMap.getOrElse(INPUT_PATH, "src/main/resources/input_data") else "src/main/resources/input_data"
    val inputPoiFile = if (optMap.contains(INPUT_POI_FILE)) optMap.getOrElse(INPUT_POI_FILE, "src/main/resources/poi.json") else "src/main/resources/poi.json"
    val daysToFilter = if (optMap.contains(DAYS_TO_FILTER)) optMap.getOrElse(DAYS_TO_FILTER, 30) else 30
    val outputPath = if (optMap.contains(OUTPUT_PATH)) optMap.getOrElse(OUTPUT_PATH, "filteredAdvertisers") else "filteredAdvertisers"

    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()

    import spark.implicits._

    val poiSchema = new StructType()
      .add("Name", StringType, false)
      .add("Latitude", DoubleType, false)
      .add("Longitude", DoubleType, false)
      .add("Radius", DoubleType, false)

    val poiDf = spark.read.option("multiline","true").schema(poiSchema).json(inputPoiFile)
    val coordinatesList: util.List[(Double, Double)] = poiDf.map(row => (row.getDouble(1), row.getDouble(2))).collectAsList()

    val coordinatesListBroadcast: Broadcast[util.List[(Double, Double)]] = spark.sparkContext.broadcast(coordinatesList)

    spark.read.parquet(inputPath)
      .filter(to_timestamp(col("location_at")).lt(current_date() - daysToFilter))
      .filter(row => {
      coordinatesListBroadcast.value.stream().anyMatch {
        case (lat: Double, lon: Double) => calculateDistance(row.getDouble(2), row.getDouble(3), lat, lon) < 50
      }
    }).show()
//      .write.parquet(outputPath)

    spark.stop()
  }

}
