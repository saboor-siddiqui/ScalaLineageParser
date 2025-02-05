import org.apache.spark.sql.DataFrame

object ChainedOperationsTest {
  def process(df: DataFrame): DataFrame = {
    df.select("col1", "col2")
      .where("col1 > 0")
      .groupBy("col2")
      .agg(Map("col1" -> "sum"))
  }

  def complexProcess(df: DataFrame): DataFrame = {
    df.select("id", "value", "category")
      .where("value >= 100")
      .groupBy("category")
      .agg(
        Map(
          "value" -> "avg",
          "id" -> "count"
        )
      )
      .orderBy("category")
  }

  def multiStepProcess(df: DataFrame): DataFrame = {
    val filteredDf = df.select("product", "quantity", "price")
      .where("quantity > 0")
    
    val aggregatedDf = filteredDf
      .groupBy("product")
      .agg(
        Map(
          "quantity" -> "sum",
          "price" -> "avg"
        )
      )
    
    aggregatedDf.where("sum(quantity) >= 10")
      .orderBy("product")
  }
}