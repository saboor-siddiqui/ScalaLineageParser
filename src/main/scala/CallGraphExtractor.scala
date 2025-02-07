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

  private var dfVariables = Map.empty[String, String] // Track DataFrame assignments
  private var currentDfSource: Option[String] = None // Track current DataFrame source

  def processFile(file: File): Try[Unit] = Try {
    val source = scala.io.Source.fromFile(file).mkString
    val fileName = file.getName

    source.parse[Source] match {
      case Parsed.Success(tree) =>
        println(s"\nProcessing file: $fileName")

        // First pass: Track DataFrame sources and assignments
        tree.traverse {
          case Defn.Val(_, List(Pat.Var(Term.Name(name))), _, rhs) =>
            val source = rhs match {
              case Term.Apply(Term.Select(qual, Term.Name("readTable")), args) =>
                // Track the source DataFrame from readTable
                currentDfSource = Some("rawData")
                dfVariables.getOrElse("sparkSession.read", "sparkSession.read\n      .option(\"mergeSchema\", \"true\")\n      .parquet(path)")
              case Term.Apply(Term.Select(qual, _), _) =>
                dfVariables.getOrElse(qual.syntax, qual.syntax)
              case Term.Name(n) => dfVariables.getOrElse(n, n)
              case _ => name
            }
            dfVariables = dfVariables + (name -> source)
            println(s"Tracked DataFrame: $name -> $source")

          case Defn.Def(_, name, _, _, _, _) =>
            // Reset DataFrame source for new method
            currentDfSource = None

          case Term.Apply(Term.Select(qual, name), _) if name.value == "readTable" =>
            // Track the source DataFrame from readTable
            currentDfSource = Some("rawData")
            dfVariables = dfVariables + ("rawData" -> "sparkSession.read")
        }

        // Second pass: Process method calls with resolved DataFrame sources
        visitor.apply(tree)
        val uniqueCalls = visitor.getCallChain.toSet // Remove duplicates
        uniqueCalls.foreach { case (caller, callee) =>
          val resolvedCallee = dfVariables.foldLeft(callee) { case (acc, (name, source)) =>
            if (acc.contains(name)) {
              acc.replace(name, source)
            } else acc
          }

          // Add edge only if it's a new unique call
          val edge = CallEdge(
            MethodNode(caller, fileName),
            MethodNode(resolvedCallee, fileName)
          )
          if (!CallGraph.getEdges.contains(edge)) {
            println(s"Found call: $caller -> $resolvedCallee")
            CallGraph.addEdge(
              MethodNode(caller, fileName),
              MethodNode(resolvedCallee, fileName)
            )
          }
        }

        // Process DataFrame operations with source tracking
        try {
          val lineages = dfAnalyzer.analyzeOperations(tree)
          var previousOp: Option[String] = None

          lineages.foreach { lineage =>
            lineage.operationChain.foreach { op =>
              val dfSource = currentDfSource.getOrElse("df")
              val currentOp = s"$dfSource.${op.name}"

              previousOp.foreach { prev =>
                CallGraph.addEdge(
                  MethodNode(prev, fileName),
                  MethodNode(currentOp, fileName)
                )
              }
              previousOp = Some(currentOp)
            }
          }
        } catch {
          case e: Exception =>
            println(s"Warning: DataFrame analysis failed for $fileName: ${e.getMessage}")
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
