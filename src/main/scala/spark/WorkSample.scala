package spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object WorkSample {

  def sparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Work Sample")
      .getOrCreate()
  }

  def readData(path: String) = {
    sparkSession.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(path)
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/Users/dheer")
    val dataSample = readData("data/DataSample.csv")
    val poiData = readData("data/POIList.csv")

    //1. Clean Up Task for suspicious records
    val cleanData: DataFrame = TasksOneAndTwo.cleanUpTask(dataSample)

    //2. Assign POI
    val poiAssigned = TasksOneAndTwo.assignPOI(cleanData, poiData)
    // poiAssigned.show(false)
    //3. Radius and Density of requests per POI.
    val radiusAndDensity = TaskThree.radiusAndDensityOfPOI(poiAssigned)
    radiusAndDensity.show(false)
  }
}
