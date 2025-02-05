import model.{DataFrameLineage, DataFrameOperation}

object DataFrameApiGenerator {
  def generate(lineages: List[DataFrameLineage]): String = {
    lineages.map { lineage =>
      // Start with the source read operation
      val sourceRead = if (lineage.methodName.contains("transformData")) {
        "sparkSession.read\n    .option(\"mergeSchema\", \"true\")\n    .parquet(path)"
      } else {
        lineage.sourceDataFrame.getOrElse("df")
      }
      
      // Get operations in correct order (reverse the chain)
      val operations = lineage.operationChain.reverse.map(operationToCode)
      
      s"""// Method: ${lineage.methodName}
         |// File: ${lineage.fileName}
         |$sourceRead${operations.mkString}""".stripMargin
    }.mkString("\n\n")
  }

  private def operationToCode(op: DataFrameOperation): String = {
    op.name match {
      case "select" =>
        val cols = op.sourceColumns.map(col => s""""$col"""").mkString(", ")
        s".select($cols)"
        
      case "where" =>
        val condition = op.condition.getOrElse("")
        s".where($condition)"
        
      case "groupBy" =>
        val cols = op.sourceColumns.map(col => s""""$col"""").mkString(", ")
        s".groupBy($cols)"
        
      case "agg" =>
        val aggregations = if (op.targetColumns.isEmpty) {
          "Map(\"col1\" -> \"sum\")"
        } else {
          op.targetColumns.map(agg => s""""$agg"""").mkString(", ")
        }
        s".agg($aggregations)"
        
      case other => s".$other(${op.sourceColumns.mkString(", ")})"
    }
  }
}