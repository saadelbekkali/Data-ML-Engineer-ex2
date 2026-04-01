package utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSparkSession(appName: String = "Default Spark App"): SparkSession = {
    System.setProperty("user.name", "saadelbekkali")
    SparkSession.builder()
      .appName(appName)
      .config("spark.master", "local[*]")
      .config("spark.hadoop.hadoop.security.authentication", "simple")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }
}