import scala.meta._

object CodeTransformer {
  def transform(source: String): String = {
    source.parse[Source].get.transform {
      case Term.Apply(
        Term.Select(Term.Name("DataReader"), Term.Name("readTable")),
        List(sparkSession, tableName)
      ) =>
        q"""${sparkSession}.read
            .option("mergeSchema", "true")
            .parquet("path/to/" + ${tableName})"""
    }.syntax
  }
}