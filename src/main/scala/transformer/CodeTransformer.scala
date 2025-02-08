package transformer

import scala.meta._

/**
 * Demonstrates pattern-based source code transformation using scala.meta.
 *
 * While this transformer shows direct AST-based transformation of specific patterns
 * (like DataReader.readTable calls), the main DataFrame API code generation is handled
 * by [[LineageTransformer]] which:
 *
 * 1. Extracts complete DataFrame operation chains
 * 2. Analyzes dependencies between operations
 * 3. Generates equivalent DataFrame API code
 *
 * Example usage:
 * {{{
 * // Direct transformation of specific patterns
 * val source = """DataReader.readTable(spark, "users")"""
 * val transformed = CodeTransformer.transform(source)
 * // Results in: spark.read.option("mergeSchema", "true").parquet("path/to/users")
 *
 * // For complete DataFrame API generation, use LineageTransformer instead:
 * // val apiCode = LineageTransformer.transform(inputDirectory)
 * }}}
 */
object CodeTransformer {
  def transform(source: String): String = {
    source.parse[Source].get.transform {
      case Term.Apply(
        Term.Select(Term.Name("DataReader"), Term.Name("readTable")),
        List(sparkSession, tableName)
      ) =>
        // Construct the code as a string
        val code = s"""${sparkSession.syntax}.read
                       .option("mergeSchema", "true")
                       .parquet("path/to/" + ${tableName.syntax})"""

        // Parse the string into a Term
        code.parse[Stat].get match {
          case term: Term => term
          case other =>
            println(s"Unexpected parse result: $other")
            Term.Name(code) // Fallback: return the code as a Term.Name
        }
    }.syntax
  }
}
