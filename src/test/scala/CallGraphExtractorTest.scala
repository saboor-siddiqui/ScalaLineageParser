import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}
import model.{MethodNode, CallEdge}
import parser.CallGraphExtractor
import parser.CallGraph
class CallGraphExtractorTest extends AnyFlatSpec with Matchers {
  
  private def createTempFile(content: String): File = {
    val temp = File.createTempFile("test", ".scala")
    temp.deleteOnExit()
    val writer = new PrintWriter(temp)
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
    temp
  }

  "CallGraphExtractor" should "extract simple function dependencies correctly" in {
    val content = """
      |object TestObject {
      |  def methodA(): Unit = {
      |    methodB()
      |  }
      |  def methodB(): Unit = {
      |    println("Hello")
      |  }
      |}
      """.stripMargin
    
    val testFile = createTempFile(content)
    val extractor = new CallGraphExtractor()
    
    extractor.processFile(testFile) match {
      case Success(_) =>
        val edges = CallGraph.getEdges
        edges should contain(CallEdge(
          MethodNode("TestObject.methodA", testFile.getName),
          MethodNode("TestObject.methodB", testFile.getName)
        ))
    }
  }

  it should "handle nested function calls correctly" in {
    val content = """
      |object TestObject {
      |  def methodA(): Unit = {
      |    methodB()
      |  }
      |  def methodB(): Unit = {
      |    methodC()
      |  }
      |  def methodC(): Unit = {
      |    println("Deep call")
      |  }
      |}
      """.stripMargin
    
    val testFile = createTempFile(content)
    val extractor = new CallGraphExtractor()
    
    extractor.processFile(testFile) match {
      case Success(_) =>
        val edges = CallGraph.getEdges
        edges should contain allOf(
          CallEdge(
            MethodNode("TestObject.methodA", testFile.getName),
            MethodNode("TestObject.methodB", testFile.getName)
          ),
          CallEdge(
            MethodNode("TestObject.methodB", testFile.getName),
            MethodNode("TestObject.methodC", testFile.getName)
          )
        )
    }
  }

  it should "detect circular dependencies" in {
    val content = """
      |object TestObject {
      |  def methodA(): Unit = {
      |    methodB()
      |  }
      |  def methodB(): Unit = {
      |    methodC()
      |  }
      |  def methodC(): Unit = {
      |    methodA()
      |  }
      |}
      """.stripMargin
    
    val testFile = createTempFile(content)
    val extractor = new CallGraphExtractor()
    
    extractor.processFile(testFile) match {
      case Success(_) =>
        val edges = CallGraph.getEdges
        edges should contain allOf(
          CallEdge(
            MethodNode("TestObject.methodA", testFile.getName),
            MethodNode("TestObject.methodB", testFile.getName)
          ),
          CallEdge(
            MethodNode("TestObject.methodB", testFile.getName),
            MethodNode("TestObject.methodC", testFile.getName)
          ),
          CallEdge(
            MethodNode("TestObject.methodC", testFile.getName),
            MethodNode("TestObject.methodA", testFile.getName)
          )
        )
    }
  }

  it should "handle invalid files gracefully" in {
    val content = "this is not valid scala code"
    val testFile = createTempFile(content)
    val extractor = new CallGraphExtractor()
    
    extractor.processFile(testFile) match {
      case Failure(exception) =>
        exception.getMessage should include("Failed to parse")
      case Success(_) =>
        fail("Expected failure for invalid Scala code")
    }
  }

  it should "handle empty files" in {
    val testFile = createTempFile("")
    val extractor = new CallGraphExtractor()
    
    extractor.processFile(testFile) match {
      case Success(_) =>
        CallGraph.getEdges shouldBe empty
      case Failure(_) =>
        fail("Should handle empty files without error")
    }
  }
}