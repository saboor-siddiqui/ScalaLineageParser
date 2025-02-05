package model

case class DataFrameOperation(
  name: String,
  sourceColumns: Set[String] = Set.empty,
  targetColumns: Set[String] = Set.empty,
  condition: Option[String] = None
)

case class DataFrameLineage(
  operationChain: List[DataFrameOperation],
  sourceDataFrame: Option[String] = None,
  sourceType: Option[String] = None,
  sourcePath: Option[String] = None,
  methodName: String,
  fileName: String
)

case class MethodNode(
  name: String, 
  file: String, 
  params: List[String] = Nil, 
  returnType: Option[String] = None
)

case class CallEdge(
  caller: MethodNode, 
  callee: MethodNode
)