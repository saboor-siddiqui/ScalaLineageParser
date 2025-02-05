import scala.meta._

/**
 * Visitor for analyzing method calls in Scala AST
 * Tracks method call relationships and builds call chains
 */
class CallGraphVisitor {
  private var callChain = List.empty[(String, String)]
  private var currentObject: Option[String] = None
  private var currentMethod: Option[String] = None

  def apply(tree: Tree): Unit = {
    tree.traverse {
      case obj: Defn.Object =>
        currentObject = Some(obj.name.value)

      case defn: Defn.Def =>
        val methodName = currentObject match {
          case Some(objName) => s"$objName.${defn.name.value}"
          case None => defn.name.value
        }
        currentMethod = Some(methodName)

      case Term.Apply(Term.Select(qual, name), _) =>
        val callerName = currentMethod.getOrElse("")
        val calleeName = s"${qual.toString}.${name.value}"
        if (callerName.nonEmpty && isTrackedMethod(calleeName)) {
          callChain = (callerName, calleeName) :: callChain
        }
    }
  }

  private def isTrackedMethod(methodName: String): Boolean = {
    val trackedPrefixes = Set(
      "DataProcessor.",
      "DataReader.",
      "StorageReader."
    )
    trackedPrefixes.exists(methodName.startsWith)
  }

  def getCallChain: List[(String, String)] = callChain.reverse

  def clear(): Unit = {
    callChain = List.empty
    currentObject = None
    currentMethod = None
  }
}