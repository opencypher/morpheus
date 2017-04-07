package org.opencypher.spark.impl.instances

package object spark {
  object all extends spark.AllInstances
  object records extends SparkCypherRecordsInstances
  object cypher extends SparkCypherInstances
}
