package org.opencypher.spark.impl.instances

import org.opencypher.spark.impl.instances.ir.block.ExprBlockInstances

package object spark {
  object all extends spark.AllInstances
  object records extends SparkCypherRecordsInstances
  object cypher extends SparkCypherInstances
}
