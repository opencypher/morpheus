package org.opencypher.spark.benchmark

import org.opencypher.spark.api.value.{CypherNode, CypherRelationship}
import org.opencypher.spark.impl.StdPropertyGraph

object RDDs {

  def nodeScanIdsSorted(label: String) = (graph: StdPropertyGraph) => {
    val ids = graph.nodes.rdd.filter(CypherNode.labels(_).exists(_.contains(label))).map(CypherNode.id(_).map(_.v).get)
    ids.sortBy(identity, ascending = false)
  }

  def simplePattern(startLabel: String = "Group", relType: String = "ALLOWED_INHERIT", endLabel: String = "Company") = (graph: StdPropertyGraph) => {
    val nodes = graph.nodes.rdd
    val relationships = graph.relationships.rdd

    val startNodeIds = nodes.filter(CypherNode.labels(_).exists(_.contains(startLabel))).map(n => CypherNode.id(n).map(_.v).get -> CypherNode.id(n).map(_.v).get)
    val endNodeIds = nodes.filter(CypherNode.labels(_).exists(_.contains(endLabel))).map(n => CypherNode.id(n).map(_.v).get -> CypherNode.id(n).map(_.v).get)
    val relStartAndEndIds = relationships.filter(CypherRelationship.relationshipType(_).exists(_ == relType)).map(r => (CypherRelationship.startId(r).get.v, r -> CypherRelationship.endId(r).get.v))
    val keyOnEnd = startNodeIds.join(relStartAndEndIds).map {
      case (startId, (_, (r, endId))) => (endId, r -> startId)
    }
    keyOnEnd.join(endNodeIds).map {
      case (endNodeId, ((r, startNodeId), _)) => r
    }
  }

}
