package org.opencypher.spark.prototype.ir

import scala.collection.immutable.SortedMap

final case class QueryDescriptor[E](
  info: QueryInfo,
  model: QueryModel[E],
  returns: SortedMap[String, Field]
)
