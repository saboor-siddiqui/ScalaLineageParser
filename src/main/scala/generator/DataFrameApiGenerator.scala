package generator

import model.{DataFrameLineage, DataFrameOperation}

object DataFrameApiGenerator {
  def generate(lineages: List[DataFrameLineage]): String = {
    lineages.map { lineage =>
      println(s"\nProcessing method: ${lineage.methodName}")
      
      val sourceRead = lineage.operationChain.headOption.flatMap(_.dfSource) match {
        case Some(source) =>
          println(s"Using original source: $source")
          source
        case None =>
          println("No source found, using default sparkSession.read chain")
          "sparkSession.read\n      .option(\"mergeSchema\", \"true\")\n      .parquet(path)"
      }
      
      println(s"Final source for ${lineage.methodName}: $sourceRead")
      val operations = lineage.operationChain.map(operationToCode)
      
      s"""// Method: ${lineage.methodName}\n$sourceRead${operations.mkString}"""
    }.mkString("\n\n")
  }

  private def operationToCode(op: DataFrameOperation): String = {
    op.name match {
      case "select" =>
        val cols = op.sourceColumns.map(col => s""""$col"""").mkString(", ")
        s""".select($cols)"""
        
      case "where" | "filter" =>
        val condition = op.condition.getOrElse("")
        if (condition.contains("col(")) s""".filter($condition)"""
        else s""".filter("$condition")"""
        
      case "groupBy" =>
        val cols = op.sourceColumns.map(col => s""""$col"""").mkString(", ")
        s""".groupBy($cols)"""
        
      case "agg" =>
        println(s"\nProcessing agg operation:")
        println(s"Source columns: ${op.sourceColumns}")
        println(s"Target columns: ${op.targetColumns}")
        println(s"Condition: ${op.condition}")
        
        val aggregations = if (op.targetColumns.nonEmpty) {
          println("Using target columns for aggregations")
          op.targetColumns.mkString(",\n        ")
        } else {
          println("Target columns empty, checking condition")
          op.condition.getOrElse {
            println("Condition empty, falling back to source columns")
            op.sourceColumns.mkString(",\n        ")
          }
        }
        
        if (aggregations.nonEmpty) {
          println(s"Final aggregations: $aggregations")
          s""".agg(\n        ${aggregations}\n      )"""
        } else {
          println("WARNING: No aggregations found!")
          println(s"Operation details:")
          println(s"Source columns: ${op.sourceColumns}")
          println(s"Target columns: ${op.targetColumns}")
          println(s"Condition: ${op.condition}")
          ".agg()"
        }
      
      case "withColumn" =>
        val colName = op.sourceColumns.headOption.getOrElse("")
        val expr = op.condition.getOrElse("")
        s""".withColumn("$colName", $expr)"""
        
      case "orderBy" =>
        val cols = op.condition.getOrElse("")
        s""".orderBy($cols)"""

      case other => s""".$other(${op.sourceColumns.mkString(", ")})"""
    }
  }
}