package org.opencypher.spark.benchmark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.opencypher.spark.api.value.{CypherNode, CypherRelationship}
import org.opencypher.spark.impl.StdPropertyGraph

object Datasets {

  def nodeScanIdsSorted(label: String, sparkSession: SparkSession) = (graph: StdPropertyGraph) => {
    import sparkSession.implicits._

    val ids = graph.nodes.filter(CypherNode.labels(_).exists(_.contains(label))).map(CypherNode.id(_).map(_.v).get)
    ids.sort(desc(ids.columns(0)))
  }

  def simplePattern(startLabel: String, relType: String, endLabel: String, sparkSession: SparkSession) = (graph: StdPropertyGraph) => {
    import sparkSession.implicits._

//    val a = graph.nodes.filter(CypherNode.labels(_).exists(_.contains(startLabel))).map(CypherNode.id(_).get.v)
//    val b = graph.nodes.filter(CypherNode.labels(_).exists(_.contains(endLabel))).map(CypherNode.id(_).get.v)
//    val rels = graph.relationships.filter(CypherRelationship.relationshipType(_).exists(_ == relType)).map(r => CypherRelationship.id(_).get.v -> r)
//
//    a.join(rels)
    ???
  }
}
