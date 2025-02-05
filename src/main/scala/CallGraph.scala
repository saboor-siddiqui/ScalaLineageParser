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

  def addEdge(caller: MethodNode, callee: MethodNode): Unit = synchronized {
    nodes += caller
    nodes += callee
    edges += CallEdge(caller, callee)
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
        // Reverse DataFrame operation edges to show execution order
        s""""${e.callee.name}" -> "${e.caller.name}""""
      } else {
        // Keep original direction for method calls
        s""""${e.caller.name}" -> "${e.callee.name}""""
      }
    }.mkString("\n  ", ";\n  ", ";")
    
    s"""digraph CallGraph {
       |  compound = true;
       |  node [style=filled,color=white];
       |$subgraphs
       |  // Edges
       |  $edgeStr
       |}""".stripMargin
  }

  def clear(): Unit = synchronized {
    nodes = Set.empty
    edges = Set.empty
    nodeMetadata = Map.empty
  }
}