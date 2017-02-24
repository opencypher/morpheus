package org.opencypher.spark.prototype.ir

final case class CypherQuery[E](
  info: QueryInfo,
  model: QueryModel[E]
)



