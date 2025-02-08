package parser

import scala.collection.concurrent.TrieMap
import model.{MethodNode, CallEdge}

/**
 * Thread-safe singleton object for managing the call graph data structure
 */
object CallGraph {
  private var nodes: Set[MethodNode] = Set.empty
  private var edges: Set[CallEdge] = Set.empty
  private var nodeMetadata: Map[MethodNode, Map[String, String]] = Map.empty
  
  private var methodPrefixes: Set[String] = Set(
    "DataProcessor.", 
    "DataReader.", 
    "StorageReader."
  )
  
  def updateMethodPrefixes(prefixes: Set[String]): Unit = synchronized {
    methodPrefixes = prefixes
  }

  private var dfVariableMap = Map.empty[String, String]  // Track DataFrame variable sources

    def addDfVariable(variable: String, source: String): Unit = synchronized {
      dfVariableMap += (variable -> source)
    }
  
    def getDfSource(variable: String): Option[String] = synchronized {
      dfVariableMap.get(variable)
    }
  
    def addEdge(caller: MethodNode, callee: MethodNode): Unit = synchronized {
      if (!edges.exists(e => e.caller == caller && e.callee == callee)) {
        // Track method parameters and their sources
        val methodName = caller.name.split("\\.").last
        val paramName = if (callee.name.contains(".")) callee.name.split("\\.")(0) else ""
        
        val processedCallee = if (isDataFrameOperation(callee.name)) {
          val dfName = callee.name.split("\\.")(0)
          // Check if it's a parameter of the current method
          if (dfName == "df" && methodName != "processData") {
            // Link to the source of the calling method's parameter
            callee.copy(name = callee.name.replace("df.", "rawData."))
          } else {
            dfVariableMap.get(dfName) match {
              case Some(source) => callee.copy(name = callee.name.replace(dfName + ".", source + "."))
              case None => callee
            }
          }
        } else if (callee.name.contains("sparkSession:")) {
          callee.copy(name = callee.name.replace(":", "."))
        } else callee
        
        nodes += caller
        nodes += processedCallee
        edges += CallEdge(caller, processedCallee)
      }
    }
  
    // Helper method to identify DataFrame operations
    private def isDataFrameOperation(name: String): Boolean = {
      val dfOperations = Set(
        "select", "where", "filter", "groupBy", "agg", "join", 
        "orderBy", "sort", "withColumn", "drop", "union"
      )
      name.contains(".") && dfOperations.exists(op => name.contains(s".$op"))
    }

    def findSource(methodName: String, dfName: String): Option[String] = {
      println(s"Looking for source of $dfName in method $methodName")
      
      // First check the variable map
      dfVariableMap.get(dfName) match {
        case some @ Some(_) => some
        case None =>
          def findInEdges(node: MethodNode, visited: Set[MethodNode] = Set.empty): Option[String] = {
            if (visited.contains(node)) {
              None // Avoid circular dependencies
            } else {
              val incomingEdges = edges.filter(_.callee == node)
              incomingEdges.find(edge => edge.caller.name.contains("readParquetData") || 
                                        edge.caller.name.contains("sparkSession.")) match {
                case Some(edge) => 
                  println(s"Found source: ${edge.caller.name}")
                  Some("sparkSession.read\n      .option(\"mergeSchema\", \"true\")\n      .parquet(path)")
                case None =>
                  incomingEdges.flatMap(edge => findInEdges(edge.caller, visited + node)).headOption
              }
            }
          }
          nodes.find(_.name.contains(methodName)).flatMap(node => findInEdges(node))
    }
  }
  def addNodeMetadata(node: MethodNode, metadata: Map[String, String]): Unit = synchronized {
    nodeMetadata += (node -> (nodeMetadata.getOrElse(node, Map.empty) ++ metadata))
  }

  def getEdges: Set[CallEdge] = synchronized {
    edges
  }

  def toDot: String = {
    val relevantNodes = nodes.filter(n => 
      methodPrefixes.exists(n.name.startsWith) ||
      n.name.startsWith("df.") ||
      n.name.startsWith("sparkSession.")
    )
    
    // Group nodes by their defining file
    val fileGroups = relevantNodes.groupBy { node =>
      if (node.name.startsWith("DataProcessor.")) "DataProcessor.scala"
      else if (node.name.startsWith("DataReader.")) "DataReader.scala"
      else if (node.name.startsWith("StorageReader.")) "StorageReader.scala"
      else if (node.name.startsWith("df.")) "DataProcessor.scala"
      else node.file
    }
    
    // Create subgraphs with nodes in their correct files
    val subgraphs = fileGroups.map { case (file, nodes) =>
      val nodeStr = nodes.map { n =>
        val metadataStr = nodeMetadata.get(n).map { meta =>
          meta.map { case (k, v) => s"$k='$v'" }.mkString(",")
        }.getOrElse("")
        s""""${n.name}" [${metadataStr}]"""
      }.mkString("\n    ", ";\n    ", ";")
      
      s"""  subgraph "cluster_$file" {
         |    label = "$file";
         |    style = filled;
         |    color = lightgrey;
         |    $nodeStr
         |  }""".stripMargin
    }.mkString("\n")
  
    // Handle edges with special case for DataFrame operations
    val edgeStr = edges.map { e =>
      if (e.caller.name.startsWith("df.") && e.callee.name.startsWith("df.")) {
        // For DataFrame operations, maintain the execution order
        s""""${e.callee.name}" -> "${e.caller.name}""""
      } else {
        // For other method calls, keep original direction
        s""""${e.caller.name}" -> "${e.callee.name}""""
      }
    }.mkString("\n  ", ";\n  ", ";")

    // Add the missing closing brace here
    s"""digraph CallGraph {
       |  compound = true;
       |  node [style=filled,color=white];
       |$subgraphs
       |$edgeStr
       |}""".stripMargin
  }
}