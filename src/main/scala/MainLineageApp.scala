import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}
import transformer.LineageTransformer

object MainLineageApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 1 || args.length > 2) {
      println("""
        |Usage: MainLineageApp <input-directory> [output-file]
        |  input-directory: Directory containing Scala source files to analyze
        |  output-file: Optional. Path to write the generated DataFrame API code
        |              If not provided, output will be printed to console
        |""".stripMargin)
      sys.exit(1)
    }

    val inputDir = args(0)
    val outputFile = if (args.length == 2) Some(args(1)) else None

    try {
      val generatedCode = LineageTransformer.transform(inputDir)
      
      outputFile match {
        case Some(filePath) =>
          Try {
            val writer = new PrintWriter(new File(filePath))
            try {
              writer.write(generatedCode)
              println(s"Generated DataFrame API code has been written to: $filePath")
            } finally {
              writer.close()
            }
          } match {
            case Success(_) => ()
            case Failure(e) => 
              System.err.println(s"Failed to write to output file: ${e.getMessage}")
              sys.exit(1)
          }
          
        case None =>
          println("Generated DataFrame API Code:")
          println("============================")
          println(generatedCode)
      }
    } catch {
      case e: IllegalArgumentException =>
        System.err.println(s"Error: ${e.getMessage}")
        sys.exit(1)
      case e: Exception =>
        System.err.println(s"An unexpected error occurred: ${e.getMessage}")
        sys.exit(1)
    }
  }
}