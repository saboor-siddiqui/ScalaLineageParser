import org.apache.spark.sql.{SparkSession, DataFrame}

object StorageReader {
  def readParquetData(sparkSession: SparkSession, path: String): DataFrame = {
    sparkSession.read
      .option("mergeSchema", "true")
      .parquet(path)
  }
}