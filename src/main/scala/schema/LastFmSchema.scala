package schema

import org.apache.spark.sql.types._

object LastFmSchema {
  
  val lastFmSchema: StructType = StructType(Array(
    StructField("user_id", StringType, true),
    StructField("timestamp", TimestampType, true),
    StructField("artist_mb_id", StringType, true),
    StructField("artist_name", StringType, true),
    StructField("track_mb_id", StringType, true),
    StructField("track_name", StringType, true)
  ))
}