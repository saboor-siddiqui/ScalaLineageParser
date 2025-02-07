import scala.meta._

/**
 * Visitor for analyzing method calls in Scala AST
 * Tracks method call relationships and builds call chains
 */
class CallGraphVisitor {
  private var callChain = Set.empty[(String, String)]  // Changed from List to Set to avoid duplicates
  private var currentObject: Option[String] = None
  private var currentMethod: Option[String] = None
  private var dfVariables = Map.empty[String, String]  // Track DataFrame variable assignments

  def apply(tree: Tree): Unit = {
    tree.traverse {
      case Defn.Object(_, name, _) =>
        currentObject = Some(name.value)
      
      case Defn.Def(_, name, _, _, _, _) =>
        currentMethod = Some(name.value)
        
      case Defn.Val(_, List(Pat.Var(Term.Name(name))), _, rhs) =>
        def extractSource(t: Term): String = t match {
          case Term.Apply(Term.Select(inner, _), _) =>
            extractSource(inner)
          case Term.Select(qual, _) =>
            extractSource(qual)
          case Term.Name(n) =>
            dfVariables.getOrElse(n, n)
          case _ =>
            t.syntax
        }
        val src = extractSource(rhs)
        dfVariables = dfVariables + (name -> src)
        println(s"Tracked DataFrame: $name -> $src")
      
      case Term.Apply(Term.Select(qual, name), args) =>
        for {
          obj <- currentObject
          method <- currentMethod
        } yield {
          val caller = s"$obj.$method"
          val qualName = dfVariables.getOrElse(qual.syntax, qual.syntax)
          val callee = s"$qualName.${name.value}"
          callChain = callChain + ((caller, callee))
        }
    }
  }

  def getCallChain: Set[(String, String)] = callChain
}