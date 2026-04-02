import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import schema.LastFmSchema
import utils.SparkUtils


object Main {

  def main(args: Array[String]): Unit = {

    val inputPath  = sys.env.getOrElse("INPUT_PATH", "/data/userid-timestamp-artid-artname-traid-traname.tsv")
    val outputPath = sys.env.getOrElse("OUTPUT_PATH", "/output/output-top10-songs")

    val spark = SparkUtils.getSparkSession("LastFM Custom App")

    import spark.implicits._

    // Read the TSV

    val plays = spark.read
      .option("sep", "\t")
      .option("header", "false")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
      .option("mode", "DROPMALFORMED")
      .schema(LastFmSchema.lastFmSchema)
      .csv(inputPath)
      .filter(col("user_id").isNotNull && col("timestamp").isNotNull && col("track_name").isNotNull)
      .select("user_id", "timestamp", "artist_name", "track_name") 

    // Assign session IDs
    // A new session starts when the gap from the previous song is > 20 minutes
    val windowUserTimestamp = Window.partitionBy("user_id").orderBy("timestamp")

    val withGap = plays
      .withColumn("prev_ts", lag("timestamp", 1).over(windowUserTimestamp))
      .withColumn("gap_seconds", unix_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'") - unix_timestamp(col("prev_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .withColumn(
        "new_session",
        when(col("prev_ts").isNull || col("gap_seconds") > 20 * 60, 1).otherwise(0)
      )


    val withSessions = withGap
      .withColumn("session_id", concat_ws("_", col("user_id"), sum("new_session").over(windowUserTimestamp)))
      .drop("prev_ts", "gap_seconds", "new_session")
      .cache()


    // Top 50 sessions by track count
    val top50 = withSessions
      .groupBy("session_id")
      .count()
      .orderBy(col("count").desc)
      .limit(50)
      .select("session_id")

    // Top 10 songs within those sessions
    val top10 = withSessions
      .join(broadcast(top50), "session_id")
      .groupBy("artist_name", "track_name")
      .count()
      .orderBy(col("count").desc)
      .limit(10)

    println("Top 10 played in the top 50 longest sessions:")
    top10.show(truncate = false)

    top10.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
      
    withSessions.unpersist()  

    spark.stop()
  }
}
