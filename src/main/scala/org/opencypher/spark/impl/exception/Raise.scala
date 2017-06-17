package org.opencypher.spark.impl.exception

import org.opencypher.spark.api.exception.SparkCypherException

object Raise {
  def duplicateEmbeddedEntityColumn(name: String) = throw new SparkCypherException(
    "The input column '$name' is used more than once to describe an embedded entity"
  )
}
