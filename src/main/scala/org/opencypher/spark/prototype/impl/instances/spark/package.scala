package org.opencypher.spark.prototype.impl.instances

package object spark {
  object all extends spark.AllInstances
  case object records extends SparkCypherRecordsInstances
  object cypher extends SparkCypherInstances
}
