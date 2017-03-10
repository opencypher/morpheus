package org.opencypher.spark.prototype.impl.instances

import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.record.SparkCypherRecords
import org.opencypher.spark.prototype.impl.classy.Transform

class SparkCypherRecordsInstances {

  implicit val sparkCypherRecordsTransform = new Transform[SparkCypherRecords] {
    override def filter(subject: SparkCypherRecords, expr: Expr): SparkCypherRecords = ???
  }
}
