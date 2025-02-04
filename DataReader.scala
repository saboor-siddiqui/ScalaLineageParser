import org.apache.spark.sql.{SparkSession, DataFrame}

object DataReader {
  def readTable(sparkSession: SparkSession, tableName: String): DataFrame = {
    StorageReader.readParquetData(sparkSession, s"path/to/$tableName")
  }
}