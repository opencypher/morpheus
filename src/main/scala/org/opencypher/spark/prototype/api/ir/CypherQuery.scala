package org.opencypher.spark.prototype.api.ir

final case class CypherQuery[E](
  info: QueryInfo,
  model: QueryModel[E]
)



