package spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object TasksOneAndTwo {

  def cleanUpTask(dataSample: DataFrame): DataFrame = {
    dataSample.dropDuplicates(Seq("TimeSt", "Latitude", "Longitude"))
  }

  def distanceFromPoi(reqLocation: (Double, Double), poiLocation: (Double, Double)): Int = {

    val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    val latitudeDist = Math.toRadians(reqLocation._1 - poiLocation._1)
    val longitudeDist = Math.toRadians(reqLocation._2 - poiLocation._2)
    val sinLatitude = Math.sin(latitudeDist / 2)
    val sinLongitude = Math.sin(longitudeDist / 2)
    val x = sinLatitude * sinLatitude +
      (Math.cos(Math.toRadians(reqLocation._1)) *
        Math.cos(Math.toRadians(poiLocation._1)) *
        sinLongitude * sinLongitude)
    val y = 2 * Math
      .atan2(Math.sqrt(x), Math.sqrt(1 - x))

    (AVERAGE_RADIUS_OF_EARTH_KM * y).toInt
  }

  def getPOIAndPOIID(poiList: List[(String, Double, Double)], latitude: Double, longitude: Double): (String, (Double, Double), Int) = {
    var distanceList: List[Int] = List()
    for (poi <- poiList) {
      distanceList = distanceList
        .:+(distanceFromPoi((latitude, longitude), (poi._2, poi._3)))
    }
    val minIndex = distanceList.zipWithIndex.min._2
    (poiList(minIndex)._1, (poiList(minIndex)._2, poiList(minIndex)._3), distanceList.zipWithIndex.min._1)
  }

  //Assign each request to the closest (i.e. minimum distance) POI (from data/POIList.csv).
  def assignPOI(cleanData: DataFrame, poiData: DataFrame) = {
    val poiRows = poiData.collect
    var poiList: List[(String, Double, Double)] = List()
    poiRows.foreach(row => {
      poiList = poiList :+ (row.getAs("POIID"),
        row.getAs("Latitude"),
        row.getAs("Longitude"))
    })
    val assignPOIUDF: UserDefinedFunction = udf((latitude: Double, longitude: Double) => {
      getPOIAndPOIID(poiList, latitude, longitude)._2
    })
    val assignPOIIDUDF: UserDefinedFunction = udf((latitude: Double, longitude: Double) => {
      getPOIAndPOIID(poiList, latitude, longitude)._1
    })
    val assignPOIDist: UserDefinedFunction = udf((latitude: Double, longitude: Double) => {
      getPOIAndPOIID(poiList, latitude, longitude)._3
    })

    cleanData
      .withColumn("POI", assignPOIUDF(col("Latitude"), col("Longitude")))
      .withColumn("POIID", assignPOIIDUDF(col("Latitude"), col("Longitude")))
      .withColumn("distPOI",assignPOIDist(col("Latitude"), col("Longitude")))
  }
}
