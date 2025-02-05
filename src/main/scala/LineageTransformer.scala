import java.io.File
import scala.meta._
import scala.util.{Try, Success, Failure}
import model.DataFrameLineage

/**
 * Transforms Scala source files into DataFrame API code by analyzing call graphs
 * and DataFrame operation lineages.
 */
object LineageTransformer {
  private val extractor = new CallGraphExtractor()
  private val analyzer = new DataFrameAnalyzer()
  
  /**
   * Processes Scala source files from the input directory and generates
   * equivalent DataFrame API code.
   *
   * @param inputDir Path to the directory containing Scala source files
   * @return Generated DataFrame API code as a string
   * @throws IllegalArgumentException if the input directory is invalid
   */
  def transform(inputDir: String): String = {
    val directory = new File(inputDir)
    if (!directory.exists || !directory.isDirectory) {
      throw new IllegalArgumentException(s"Invalid input directory: $inputDir")
    }

    var lineages = List.empty[DataFrameLineage]

    def processFile(file: File): Unit = {
      if (file.getName.endsWith(".scala")) {
        val source = scala.io.Source.fromFile(file).mkString
        source.parse[scala.meta.Source] match {
          case scala.meta.Parsed.Success(tree) =>
            // Process call graph
            extractor.processFile(file)
            
            // Analyze DataFrame operations
            val fileLineages = analyzer.analyzeOperations(tree)
            lineages = lineages ++ fileLineages
            
          case error: scala.meta.Parsed.Error =>
            println(s"Failed to parse ${file.getName}: ${error.message}")
        }
      }
    }

    def processRecursively(dir: File): Unit = {
      dir.listFiles.foreach { file =>
        if (file.isDirectory) {
          processRecursively(file)
        } else {
          processFile(file)
        }
      }
    }

    Try {
      processRecursively(directory)
      DataFrameApiGenerator.generate(lineages)
    } match {
      case Success(code) => code
      case Failure(e) => 
        throw new RuntimeException(s"Failed to transform source files: ${e.getMessage}", e)
    }
  }
}