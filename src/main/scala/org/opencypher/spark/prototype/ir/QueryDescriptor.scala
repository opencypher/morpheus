package org.opencypher.spark.prototype.ir

final case class QueryDescriptor[E](
  info: QueryInfo,
  model: QueryModel[E]
)



