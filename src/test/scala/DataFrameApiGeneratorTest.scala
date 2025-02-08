import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import model.{DataFrameLineage, DataFrameOperation}
import generator.DataFrameApiGenerator 

class DataFrameApiGeneratorTest extends AnyFlatSpec with Matchers {
  
  "DataFrameApiGenerator" should "generate code from a simple lineage" in {
    val lineage = DataFrameLineage(
      operationChain = List(
        DataFrameOperation("select", Set("col1", "col2")),
        DataFrameOperation("where", condition = Some("col1 > 0"))
      ),
      sourceDataFrame = Some("sourceDF"),
      methodName = "testMethod",
      fileName = "TestFile.scala"
    )
    
    val generatedCode = DataFrameApiGenerator.generate(List(lineage))
    
    generatedCode should include ("sourceDF")
    generatedCode should include ("select(\"col1\", \"col2\")")
    generatedCode should include ("where(col1 > 0)")
    generatedCode should include ("testMethod")
    generatedCode should include ("TestFile.scala")
  }

  it should "generate code for multiple operations including groupBy and aggregation" in {
    val lineage = DataFrameLineage(
      operationChain = List(
        DataFrameOperation("select", Set("department", "salary")),
        DataFrameOperation("groupBy", Set("department")),
        DataFrameOperation("agg", Set("avg(salary)"))
      ),
      sourceDataFrame = Some("employeeDF"),
      methodName = "calculateAvgSalary",
      fileName = "SalaryAnalysis.scala"
    )
    
    val generatedCode = DataFrameApiGenerator.generate(List(lineage))
    
    generatedCode should include ("employeeDF")
    generatedCode should include ("select(\"department\", \"salary\")")
    generatedCode should include ("groupBy(\"department\")")
    generatedCode should include ("agg(\"avg(salary)\")")
  }

  it should "handle empty column sets by using *" in {
    val lineage = DataFrameLineage(
      operationChain = List(
        DataFrameOperation("select")
      ),
      sourceDataFrame = Some("df"),
      methodName = "selectAll",
      fileName = "Test.scala"
    )
    
    val generatedCode = DataFrameApiGenerator.generate(List(lineage))
    
    generatedCode should include ("select(*)")
  }

  it should "generate code for multiple lineages" in {
    val lineages = List(
      DataFrameLineage(
        operationChain = List(DataFrameOperation("select", Set("col1"))),
        sourceDataFrame = Some("df1"),
        methodName = "method1",
        fileName = "File1.scala"
      ),
      DataFrameLineage(
        operationChain = List(DataFrameOperation("select", Set("col2"))),
        sourceDataFrame = Some("df2"),
        methodName = "method2",
        fileName = "File2.scala"
      )
    )
    
    val generatedCode = DataFrameApiGenerator.generate(lineages)
    
    generatedCode should include ("df1")
    generatedCode should include ("df2")
    generatedCode should include ("method1")
    generatedCode should include ("method2")
    generatedCode should include ("File1.scala")
    generatedCode should include ("File2.scala")
  }
}