import scala.meta._
import model.{DataFrameOperation, DataFrameLineage}

/**
 * Analyzer for Spark DataFrame operations in Scala code
 * Tracks chains of DataFrame operations and their relationships
 */
class DataFrameAnalyzer {
  /**
   * Checks if an operation is a DataFrame transformation
   * @param name The operation name to check
   * @return true if the operation is a supported DataFrame operation
   */
  private def isDataFrameOp(name: String): Boolean = {
    Set("select", "where", "groupBy", "agg", "orderBy", "filter", "withColumn").contains(name)
  }

  def analyzeOperations(tree: Tree): List[DataFrameLineage] = {
    var methodLineages = List.empty[DataFrameLineage]
    var operations = List.empty[DataFrameOperation]
    var currentMethod: Option[String] = None
    var fileName: Option[String] = None
  
    tree.traverse {
      case Defn.Def(_, name, _, _, _, _) =>
        if (operations.nonEmpty && currentMethod.isDefined) {
          methodLineages = DataFrameLineage(
            operationChain = operations.reverse,  // Reverse the operations to maintain correct order
            methodName = currentMethod.get,
            fileName = fileName.getOrElse("")
          ) :: methodLineages
        }
        currentMethod = Some(name.value)
        operations = List.empty
    
      case Term.Apply(Term.Select(qual, name), args) if isDataFrameOp(name.value) =>
        val operation = DataFrameOperation(
          name = name.value,
          sourceColumns = extractSourceColumns(args),
          targetColumns = if (name.value == "agg") extractAggregations(args) else Set.empty,
          condition = extractCondition(args)
        )
        operations = operation :: operations  // Prepend instead of append to maintain order
    }
    
    // Handle the last method
    if (operations.nonEmpty && currentMethod.isDefined) {
      methodLineages = DataFrameLineage(
        operationChain = operations.reverse,  // Reverse the operations to maintain correct order
        methodName = currentMethod.get,
        fileName = fileName.getOrElse("")
      ) :: methodLineages
    }
    
    methodLineages.reverse  // Reverse the final list to maintain method order
  }

  private def extractAggregations(args: List[Term]): Set[String] = {
    println(s"Analyzing aggregation args: ${args.map(_.structure).mkString("\n")}")
    
    val aggregations = args.flatMap {
      // Handle single aggregation with alias: sum("col1").as("total_col1")
      case Term.Apply(
        Term.Select(
          Term.Apply(Term.Name(func), List(Lit.String(col))),
          Term.Name("as")
        ),
        List(Lit.String(alias))
      ) =>
        println(s"Matched aggregation: $func($col).as($alias)")
        Some(s"""$func("$col").as("$alias")""")

      // Handle sequence of aggregations
      case Term.Apply(_, aggs @ List(_*)) =>
        println(s"Found sequence of aggregations: ${aggs.map(_.structure).mkString(", ")}")
        aggs.flatMap {
          case Term.Apply(
            Term.Select(
              Term.Apply(Term.Name(func), List(Lit.String(col))),
              Term.Name("as")
            ),
            List(Lit.String(alias))
          ) =>
            println(s"Processing aggregation in sequence: $func($col).as($alias)")
            Some(s"""$func("$col").as("$alias")""")
          case other =>
            println(s"Unmatched sequence item: ${other.structure}")
            None
        }

      case other =>
        println(s"Unmatched top-level pattern: ${other.structure}")
        None
    }.toSet

    println(s"Extracted aggregations: $aggregations")
    aggregations
  }

  private def extractSourceColumns(args: List[Term]): Set[String] = {
      args.flatMap {
        case Lit.String(col) => Some(col)
        case Term.Apply(Term.Name(func), List(Lit.String(col))) => Some(col)
        case Term.Apply(Term.Name("desc"), List(Lit.String(col))) => Some(col)
        case Term.Select(Term.Name("col"), Term.Name(colName)) => Some(colName)
        case Term.Apply(Term.Name("datediff"), List(col1, col2)) => 
          Set(col1.syntax, col2.syntax)
        case _ => None
      }.toSet
    }

  private def extractCondition(args: List[Term]): Option[String] = {
    args.collectFirst {
      case Term.Apply(Term.Name("desc"), List(Lit.String(col))) => s"""desc("$col")"""
      case Term.Apply(Term.Name("col"), List(Lit.String(colName))) => s"""col("$colName")"""
      case Term.Apply(_, List(Term.Select(Term.Name("col"), Term.Name(colName)))) => 
        s"""col("$colName")"""
      case Lit.String(condition) => condition
      case other => other.syntax
    }
  }

  def getOperationEdges(lineages: List[DataFrameLineage]): List[(String, String)] = {
    lineages.flatMap { lineage =>
      lineage.operationChain.sliding(2).map {
        case List(prev, next) => (prev.name, next.name)
        case _ => ("", "")  // Handle single operation case
      }.filter(_._1.nonEmpty)
    }
  }
}