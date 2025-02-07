/**
 * Core component for extracting and generating call graphs from Scala source code.
 * This file contains the main logic for parsing source files and building the call graph.
 * 
 * The extractor integrates with DataFrameAnalyzer to collect DataFrame operation lineage
 * which is later used by DataFrameApiGenerator to generate equivalent DataFrame API code.
 */

import scala.meta._
import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}
import model.{MethodNode, CallEdge}

/**
 * Represents a method node in the call graph
 * @param name The fully qualified method name
 * @param file Source file containing the method
 * @param params List of parameter types
 * @param returnType Optional return type
 */
case class MethodNode(name: String, file: String, params: List[String] = Nil, returnType: Option[String] = None)

/**
 * Represents a directed edge between two method nodes in the call graph
 */
case class CallEdge(caller: MethodNode, callee: MethodNode)

/**
 * Main class for extracting call graphs from Scala source files
 * Handles file traversal, AST parsing, and graph construction
 */
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
  /**
   * Identifies DataFrame operations that should be tracked for lineage
   * This set of operations is used in conjunction with DataFrameAnalyzer
   * to build the complete operation chain for API generation
   */
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
        
        // Process DataFrame operations with enhanced lineage tracking
        val lineages = dfAnalyzer.analyzeOperations(tree)
        dfAnalyzer.getOperationEdges(lineages).foreach { case (prev, next) =>
          CallGraph.addEdge(
            MethodNode(s"df.$prev", fileName),
            MethodNode(s"df.$next", fileName)
          )
        }
        
        // Add lineage metadata to nodes for API generation
        lineages.foreach { lineage =>
          lineage.operationChain.foreach { op =>
            val node = MethodNode(s"df.${op.name}", fileName)
            CallGraph.addNodeMetadata(node, Map(
              "sourceColumns" -> op.sourceColumns.mkString(","),
              "condition" -> op.condition.getOrElse("")
            ))
          }
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

/**
 * Entry point for the call graph extraction tool
 * Usage: CallGraphExtractor <source-dir> <output-dot-file>
 */
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
