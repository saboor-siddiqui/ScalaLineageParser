import scala.meta._

class CallGraphVisitor extends Traverser {
  private var currentClass: Option[String] = None
  private var currentMethod: Option[String] = None
  private var callChain = List.empty[(String, String)]

  override def apply(tree: Tree): Unit = {
    tree match {
      case Defn.Object(_, name, _) =>
        currentClass = Some(name.value)
        super.apply(tree)
        
      case Defn.Def(_, name, _, _, _, body) =>
        val methodName = s"${currentClass.getOrElse("")}.${name.value}"
        val prevMethod = currentMethod
        currentMethod = Some(methodName)
        super.apply(body)
        currentMethod = prevMethod
        
      case Term.Apply(Term.Select(qual, name), _) =>
        currentMethod.foreach { caller =>
          val callee = s"${qual}.${name.value}"
          if (isRelevantCall(callee)) {
            callChain = (caller, callee) :: callChain
          }
        }
        super.apply(tree)
        
      case _ => super.apply(tree)
    }
  }

  private def isRelevantCall(methodName: String): Boolean = {
    methodName.startsWith("DataProcessor.") ||
    methodName.startsWith("DataReader.") ||
    methodName.startsWith("StorageReader.")
  }

  def getCallChain: List[(String, String)] = callChain.reverse
}