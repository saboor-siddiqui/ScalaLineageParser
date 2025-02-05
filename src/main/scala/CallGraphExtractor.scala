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
  
  private var methodPrefixes: Set[String] = Set(
    "DataProcessor.", 
    "DataReader.", 
    "StorageReader."
  )
  
  def updateMethodPrefixes(prefixes: Set[String]): Unit = synchronized {
    methodPrefixes = prefixes
  }

  // Make addEdge public
  def addEdge(caller: MethodNode, callee: MethodNode): Unit = synchronized {
    nodes += caller
    nodes += callee
    edges += CallEdge(caller, callee)
  }

  def toDot: String = {
    val relevantNodes = nodes.filter(n => 
      methodPrefixes.exists(n.name.startsWith) ||
      n.name.startsWith("df.") ||
      n.name.startsWith("sparkSession.")
    )
    
    val fileGroups = relevantNodes.groupBy(_.file).filter(_._1.nonEmpty)
    
    val subgraphs = fileGroups.map { case (file, fileNodes) =>
      val nodeStr = fileNodes.map { n =>
        s""""${n.name}""""
      }.mkString("\n    ", ";\n    ", ";")
      
      s"""  subgraph "cluster_${file.replace('.', '_')}" {
         |    label = "$file";
         |    style = filled;
         |    color = lightgrey;
         |    $nodeStr
         |  }""".stripMargin
    }.mkString("\n")

    val edgeStr = edges.map { e =>
      s""""${e.caller.name}" -> "${e.callee.name}""""
    }.mkString("\n  ", ";\n  ", ";")
    
    s"""digraph CallGraph {
       |  compound = true;
       |  node [style=filled,color=white];
       |$subgraphs
       |  // Edges
       |  $edgeStr
       |}""".stripMargin
  }
}

class CallGraphExtractor {
  private val visitor = new CallGraphVisitor()
  private val dfAnalyzer = new DataFrameAnalyzer()
  
  // Add missing variable declarations
  private var currentObject: Option[String] = None
  private var currentMethod: Option[MethodNode] = None
  private var methodStack: List[MethodNode] = Nil
  private var dfChain: List[MethodNode] = Nil
  private var sparkChain: List[MethodNode] = Nil

  // Add missing method definitions
  private def isMainMethod(qual: String, method: String): Boolean = {
    (qual == "DataProcessor" && Set("processData", "transformData").contains(method)) ||
    (qual == "DataReader" && method == "readTable") ||
    (qual == "StorageReader" && method == "readParquetData")
  }

  private def isSparkOp(qual: String, method: String): Boolean = {
    (qual.contains("sparkSession") || qual.contains("read")) && 
    Set("read", "option", "parquet").contains(method)
  }

  // Add isDataFrameOp to DataFrameAnalyzer
  private def isDataFrameOp(name: String): Boolean = {
    Set("select", "where", "groupBy", "agg").contains(name)
  }

  // Add missing processDirectory method
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

  def processFile(file: File): Try[Unit] = Try {
    val source = scala.io.Source.fromFile(file).mkString
    val fileName = file.getName
    
    source.parse[Source] match {
      case Parsed.Success(tree) => 
        // Process method calls
        visitor.apply(tree)
        visitor.getCallChain.foreach { case (caller, callee) =>
          CallGraph.addEdge(
            MethodNode(caller, fileName),
            MethodNode(callee, fileName)
          )
        }
        
        // Process DataFrame operations
        dfAnalyzer.analyzeOperations(tree).foreach { case (prev, next) =>
          CallGraph.addEdge(
            MethodNode(s"df.$prev", fileName),
            MethodNode(s"df.$next", fileName)
          )
        }
        
      case error: Parsed.Error => 
        throw new RuntimeException(s"Failed to parse $fileName: ${error.message}")
    }
  }

  private def traverseTree(tree: Tree, fileName: String): Unit = {
    tree.traverse {
      case obj: Defn.Object =>
        currentObject = Some(obj.name.value)
        
      case defn: Defn.Def =>
        val methodName = currentObject match {
          case Some(objName) => s"$objName.${defn.name.value}"
          case None => defn.name.value
        }
        val method = MethodNode(methodName, fileName)
        currentMethod = Some(method)
        methodStack = method :: methodStack
        dfChain = Nil
        sparkChain = Nil

      case Term.Apply(Term.Select(qual, name), args) =>
        val methodName = name.value
        val qualName = qual.toString
        
        if (isMainMethod(qualName, methodName)) {
          val callee = MethodNode(s"$qualName.$methodName", fileName)
          methodStack.headOption.foreach { caller =>
            CallGraph.addEdge(caller, callee)
          }
          currentMethod = Some(callee)
          methodStack = callee :: methodStack
        }
        else if (isSparkOp(qualName, methodName)) {
          val sparkOp = MethodNode(s"sparkSession.$methodName", fileName)
          if (sparkChain.isEmpty) {
            methodStack.headOption.foreach(m => CallGraph.addEdge(m, sparkOp))
          } else {
            CallGraph.addEdge(sparkChain.head, sparkOp)
          }
          sparkChain = sparkOp :: sparkChain
        }
        else if (isDataFrameOp(methodName)) {
          val dfOp = MethodNode(s"df.$methodName", fileName)
          currentMethod.foreach { m =>
            if (m.name.contains("transformData")) {
              if (dfChain.isEmpty) {
                CallGraph.addEdge(m, dfOp)
              } else {
                CallGraph.addEdge(dfChain.head, dfOp)
              }
              dfChain = dfOp :: dfChain
            }
          }
        }

      case Term.Block(_) =>
        if (methodStack.nonEmpty) {
          val currentMethodNode = methodStack.head
          if (currentMethodNode.name.contains("transformData") && dfChain.nonEmpty) {
            CallGraph.addEdge(currentMethodNode, dfChain.last)
          }
          if (currentMethodNode.name.contains("readParquetData") && sparkChain.nonEmpty) {
            CallGraph.addEdge(currentMethodNode, sparkChain.last)
          }
          methodStack = methodStack.tail
          currentMethod = methodStack.headOption
          dfChain = Nil
          sparkChain = Nil
        }
    }
  }
}
object CallGraphExtractor extends App {
  val extractor = new CallGraphExtractor()
  
  if (args.length != 2) {
    println("Usage: CallGraphExtractor <source-dir> <output-dot-file>")
    sys.exit(1)
  }

  // Configure method prefixes if needed
  CallGraph.updateMethodPrefixes(Set(
    "DataProcessor.",
    "DataReader.",
    "StorageReader.",
    // Add your custom prefixes here
    "MyService.",
    "Repository."
  ))

  val sourceDir = new File(args(0))
  val outputFile = args(1)

  try {
    extractor.processDirectory(sourceDir)
    val writer = new PrintWriter(outputFile)
    try {
      writer.write(CallGraph.toDot)
    } finally {
      writer.close()
    }
    println(s"Call graph has been written to $outputFile")
  } catch {
    case e: Exception =>
      System.err.println(s"Error: ${e.getMessage}")
      if (e.getCause != null) {
        System.err.println(s"Caused by: ${e.getCause.getMessage}")
      }
      sys.exit(1)
  }
}