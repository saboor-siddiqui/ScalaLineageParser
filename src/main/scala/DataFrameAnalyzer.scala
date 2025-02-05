import scala.meta._

class DataFrameAnalyzer {
  // Add isDataFrameOp method here
  private def isDataFrameOp(name: String): Boolean = {
    Set("select", "where", "groupBy", "agg").contains(name)
  }

  def analyzeOperations(tree: Tree): List[(String, String)] = {
    var operations = List.empty[(String, String)]
    var currentChain = List.empty[String]

    tree.traverse {
      case Term.Apply(Term.Select(qual, name), _) if isDataFrameOp(name.value) =>
        val op = name.value
        currentChain = op :: currentChain
    }
    
    currentChain.sliding(2).map {
      case List(next, prev) => (prev, next)
      case _ => throw new IllegalStateException("Invalid chain state")
    }.toList
  }
}