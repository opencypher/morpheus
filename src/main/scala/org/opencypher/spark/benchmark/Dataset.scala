package org.opencypher.spark.benchmark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.opencypher.spark.api.value.CypherNode
import org.opencypher.spark.impl.StdPropertyGraph

object Dataset {

  def nodeScanIdsSorted(label: String, sparkSession: SparkSession) = (graph: StdPropertyGraph) => {
    import sparkSession.implicits._

    val ids = graph.nodes.filter(CypherNode.labels(_).exists(_.contains(label))).map(CypherNode.id(_).map(_.v).get)
    ids.sort(desc(ids.columns(0)))
  }
}
