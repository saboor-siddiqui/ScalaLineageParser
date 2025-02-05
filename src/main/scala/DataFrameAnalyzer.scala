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
    Set("select", "where", "groupBy", "agg").contains(name)
  }

  /**
   * Analyzes the AST to extract DataFrame operation chains
   * @param tree The parsed Scala AST
   * @return List of tuples representing operation edges (prev -> next)
   */
  def analyzeOperations(tree: Tree): List[DataFrameLineage] = {
    var operations = List.empty[DataFrameOperation]
    var currentMethod: Option[String] = None
    var fileName: Option[String] = None

    tree.traverse {
      case Defn.Def(_, name, _, _, _, _) =>
        currentMethod = Some(name.value)
        
      case Term.Apply(Term.Select(qual, name), args) if isDataFrameOp(name.value) =>
        val operation = DataFrameOperation(
          name = name.value,
          sourceColumns = extractSourceColumns(args),
          condition = extractCondition(args)
        )
        operations = operation :: operations
    }

    if (operations.nonEmpty && currentMethod.isDefined) {
      List(DataFrameLineage(
        operationChain = operations.reverse,
        methodName = currentMethod.get,
        fileName = fileName.getOrElse("")
      ))
    } else Nil
  }

  def getOperationEdges(lineages: List[DataFrameLineage]): List[(String, String)] = {
    lineages.flatMap { lineage =>
      lineage.operationChain.sliding(2).collect {
        case List(prev, next) => (prev.name, next.name)
      }
    }
  }

  private def extractSourceColumns(args: List[Term]): Set[String] = {
    args.flatMap {
      case Lit.String(col) => Some(col)
      case _ => None
    }.toSet
  }

  private def extractCondition(args: List[Term]): Option[String] = {
    args.collectFirst {
      case Lit.String(cond) => cond
    }
  }
}