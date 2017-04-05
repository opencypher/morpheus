package org.opencypher.spark.api.ir

final case class CypherQuery[E](
  info: QueryInfo,
  model: QueryModel[E]
)



