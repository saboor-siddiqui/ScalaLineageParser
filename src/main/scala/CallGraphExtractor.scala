import scala.meta._
import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}

// Define a simple representation for a call graph node
case class MethodNode(name: String, file: String, params: List[String] = Nil, returnType: Option[String] = None)
case class CallEdge(caller: MethodNode, callee: MethodNode)

// Call graph data structure with thread-safe operations
object CallGraph {
  private var nodes: Set[MethodNode] = Set.empty
  private var edges: Set[CallEdge] = Set.empty

  def addEdge(caller: MethodNode, callee: MethodNode): Unit = synchronized {
    nodes += caller
    nodes += callee
    edges += CallEdge(caller, callee)
  }

  def toDot: String = {
    val nodeStr = nodes.map { n =>
      s""""${n.name}""""
    }.mkString("\n  ", ";\n  ", ";")
    
    val edgeStr = edges.map { e =>
      s""""${e.caller.name}" -> "${e.callee.name}""""
    }.mkString("\n  ", ";\n  ", ";")
    
    s"""digraph CallGraph {
       |  // Nodes
       |  $nodeStr
       |  // Edges
       |  $edgeStr
       |}""".stripMargin
  }
}

class CallGraphExtractor {
  private var currentMethod: Option[MethodNode] = None
  private var currentObject: Option[String] = None
  private var dfChain: List[MethodNode] = Nil
  private var sparkChain: List[MethodNode] = Nil

  private def traverseTree(tree: Tree, fileName: String): Unit = {
    tree.traverse {
      case obj: Defn.Object =>
        currentObject = Some(obj.name.value)
        
      case defn: Defn.Def =>
        val methodName = currentObject match {
          case Some(objName) => s"$objName.${defn.name.value}"
          case None => defn.name.value
        }
        currentMethod = Some(MethodNode(methodName, ""))
        dfChain = Nil

      case Term.Apply(Term.Select(qual, name), args) =>
        val methodName = name.value
        val qualName = qual.toString
        
        if (isMainMethod(qualName, methodName)) {
          val callee = MethodNode(s"$qualName.$methodName", "")
          currentMethod.foreach { caller =>
            CallGraph.addEdge(caller, callee)
            currentMethod = Some(callee)
          }
          dfChain = Nil
          sparkChain = Nil
        }
        // Handle Spark session operations with proper chaining
        else if (isSparkOp(qualName, methodName)) {
          val sparkOp = MethodNode(s"sparkSession.$methodName", "")
          if (sparkChain.isEmpty) {
            currentMethod.foreach(m => CallGraph.addEdge(m, sparkOp))
          } else {
            CallGraph.addEdge(sparkChain.last, sparkOp)
          }
          sparkChain = sparkChain :+ sparkOp
        }
        // Handle DataFrame operations with proper chaining
        else if (isDataFrameOp(methodName)) {
          val newOp = MethodNode(s"df.$methodName", "")
          if (dfChain.isEmpty) {
            currentMethod.foreach(m => CallGraph.addEdge(m, newOp))
          } else {
            CallGraph.addEdge(dfChain.last, newOp)
          }
          dfChain = dfChain :+ newOp
        }
    }
  }

  private def isSparkOp(qual: String, method: String): Boolean = {
    (qual.contains("sparkSession") || qual.contains("read")) && 
    Set("read", "option", "parquet").contains(method)
  }

  private def isMainMethod(qual: String, method: String): Boolean = {
    (qual == "DataProcessor" && Set("processData", "transformData").contains(method)) ||
    (qual == "DataReader" && method == "readTable") ||
    (qual == "StorageReader" && method == "readParquetData")
  }

  private def isDataFrameOp(name: String): Boolean = {
    Set("select", "where", "groupBy", "agg").contains(name)
  }

  private def isObjectCall(qual: String): Boolean = {
    List("DataProcessor", "DataReader", "StorageReader").exists(qual.contains)
  }

  private def isDataFrameOperation(name: String): Boolean = {
    val dfOps = Set(
      "select", "where", "groupBy", "agg", "join",
      "filter", "read", "parquet", "option",
      "transformData", "readTable", "readParquetData"
    )
    dfOps.contains(name.toLowerCase)
  }

  private def isObjectMethodCall(qual: String): Boolean = {
    Set(
      "DataProcessor", "DataReader", "StorageReader",
      "sparkSession", "df"
    ).exists(qual.contains)
  }

  private def extractMethodCall(fun: Term): Option[String] = fun match {
    case Term.Select(qual, name) => Some(s"${qual}.${name.value}")
    case name: Term.Name => Some(name.value)
    case _ => None
  }

  // Enhanced method information extraction
  case class MethodInfo(params: List[String] = Nil, returnType: Option[String] = None)

  private def extractMethodInfo(apply: Term.Apply): (String, MethodInfo) = {
    val methodName = apply.fun match {
      case Term.Select(qual, name) => s"${qual}.${name}"
      case name: Term.Name => name.value
      case _ => apply.fun.toString
    }

    val paramTypes = apply.args.map {
      case lit: Lit => lit.syntax
      case term => term.toString
    }

    (methodName, MethodInfo(paramTypes, Some("DataFrame")))
  }

  // Process all Scala files in a directory recursively
  def processDirectory(dir: File): Unit = {
    if (!dir.exists || !dir.isDirectory) {
      throw new IllegalArgumentException(s"${dir.getPath} is not a valid directory")
    }

    def processRecursively(file: File): Unit = {
      if (file.isDirectory) {
        file.listFiles.foreach(processRecursively)
      } else if (file.getName.endsWith(".scala")) {
        processFile(file) match {
          case Success(_) => println(s"Successfully processed ${file.getName}")
          case Failure(e) => println(s"Failed to process ${file.getName}: ${e.getMessage}")
        }
      }
    }

    processRecursively(dir)
  }

  // Process a single source file
  def processFile(file: File): Try[Unit] = Try {
    val source = scala.io.Source.fromFile(file).mkString
    val fileName = file.getName
    
    source.parse[Source] match {
      case Parsed.Success(tree) => traverseTree(tree, fileName)
      case error: Parsed.Error => throw new RuntimeException(s"Failed to parse $fileName: ${error.message}")
    }
  }
} // end of CallGraphExtractor class

object CallGraphExtractor extends App {
  val extractor = new CallGraphExtractor()
  
  if (args.length != 2) {
    println("Usage: CallGraphExtractor <source-dir> <output-dot-file>")
    sys.exit(1)
  }

  val sourceDir = new File(args(0))
  val outputFile = args(1)

  try {
    // Process all Scala files in the directory
    extractor.processDirectory(sourceDir)

    // Write the DOT output to a file
    val writer = new PrintWriter(outputFile)
    writer.write(CallGraph.toDot)
    writer.close()

    println(s"Call graph has been written to $outputFile")
  } catch {
    case e: Exception =>
      println(s"Error: ${e.getMessage}")
      sys.exit(1)
  }
}