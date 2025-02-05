import org.apache.spark.sql.{SparkSession, DataFrame}

object DataProcessor {
  def processData(sparkSession: SparkSession, tableName: String): DataFrame = {
    val rawData = DataReader.readTable(sparkSession, tableName)
    transformData(rawData)
  }

  private def transformData(df: DataFrame): DataFrame = {
    df.select("col1", "col2")
      .where("col1 > 0")
      .groupBy("col2")
      .agg(Map("col1" -> "sum"))
  }
}