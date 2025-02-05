import org.apache.spark.sql.DataFrame

object SimpleTest {
  def process(df: DataFrame): DataFrame = {
    df.select("col1", "col2")
      .where("col1 > 0")
  }
}