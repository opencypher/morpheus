package org.opencypher.spark.prototype.api.spark

import org.opencypher.spark.prototype.api.graph.CypherView

trait SparkCypherView extends CypherView {
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords

  override def show() = records.toDF.show()
}
