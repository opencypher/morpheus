package org.opencypher.spark.prototype.ir

// 2) Should we tokenize fields and vars?
// 3) Blocks: Should where be part of binds? // Optional Match
// 4) Blocks: Select(?)
final case class QueryDescriptor[E](
  info: QueryInfo,
  model: QueryModel[E],
  returns: QueryReturns
)



