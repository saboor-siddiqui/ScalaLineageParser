import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test suite for LineageTransformer
 * 
 * Note: Before running tests, ensure test input files are present in:
 * src/test/resources/test_inputs/
 */
class LineageTransformerTest extends AnyFlatSpec with Matchers {
  
  "LineageTransformer" should "parse DataFrame operations from valid directory" in {
    val inputDir = "src/test/resources/test_inputs"
    val result = LineageTransformer.transform(inputDir)
    
    // Verify presence of common DataFrame operations
    result should include ("select")
    result should include ("where")
    result should include ("groupBy")
    result should not be empty
  }
  
  it should "handle invalid input directory gracefully" in {
    val invalidDir = "nonexistent/directory"
    
    val exception = intercept[RuntimeException] {
      LineageTransformer.transform(invalidDir)
    }
    
    exception.getMessage should include ("Failed to transform source files")
  }
  
  it should "process nested directories correctly" in {
    val inputDir = "src/test/resources/test_inputs/nested"
    val result = LineageTransformer.transform(inputDir)
    
    result should not be empty
    result should include ("DataFrame")
  }
  
  it should "handle empty directory" in {
    val emptyDir = "src/test/resources/test_inputs/empty"
    val result = LineageTransformer.transform(emptyDir)
    
    result shouldBe ""  // Changed from 'result should be empty'
  }
  
  it should "verify complex DataFrame operations" in {
    val inputDir = "src/test/resources/test_inputs"
    val result = LineageTransformer.transform(inputDir)
    
    // Verify more complex DataFrame operations
    result should include ("join")
    result should include ("orderBy")
    result should include ("agg")
    result should include ("filter")
  }
}