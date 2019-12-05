package spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TaskThree {

  def radiusAndDensityOfPOI(poiAssignedDf: DataFrame): DataFrame = {
    poiAssignedDf
      .groupBy(col("POIID"))
      .agg(max("distPOI").as("Radius"),
        stddev_samp("distPOI"),
        mean("distPOI"),
        count("POIID").as("Count"))
      .withColumn("Density",
        lit(col("Count")
          .divide(col("Radius")
            .multiply(col("Radius")
              .multiply(java.lang.Math.PI)))))


  }
}
