import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object DataProcessor {
  def processData(sparkSession: SparkSession, tableName: String): DataFrame = {
    val rawData = DataReader.readTable(sparkSession, tableName)
    transformData(rawData)
    aggregateByCategories(rawData)
    newMethodAdded(rawData)
  }

  private def transformData(df: DataFrame): DataFrame = {
    df.select("col1", "col2", "col3")
      .groupBy("col2")
      .agg(
        sum("col1").as("total_col1"),
        count("col3").as("count_col3"),
        avg("col1").as("avg_col1")
      )
      .orderBy(desc("total_col1"))
      .filter("count_col3 > 10")
  }

  def aggregateByCategories(df: DataFrame): DataFrame = {
    df.groupBy("category", "sub_category")
       .agg(
         sum("amount").as("total_amount"),
         count("transaction_id").as("transaction_count"),
         max("transaction_date").as("last_transaction"),
         min("transaction_date").as("first_transaction"),
         avg("amount").as("avg_amount")
       )
       .withColumn("days_active", 
         datediff(col("last_transaction"), col("first_transaction"))
       )
       .filter(col("transaction_count") > 5)
       .orderBy(desc("total_amount"))
  }

  def newMethodAdded(df: DataFrame): DataFrame = {
    df.select("col1", "col2", "col3")
      .groupBy("category", "sub_category")
      .agg(
        sum("amount").as("total_amount"),
        count("transaction_id").as("transaction_count"),
        avg("amount").as("avg_amount")
      )
      .filter(col("transaction_count") > 15)
      .orderBy(desc("total_amount"))
  }
}