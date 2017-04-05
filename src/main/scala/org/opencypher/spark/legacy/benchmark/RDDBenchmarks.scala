package org.opencypher.spark.legacy.benchmark

import org.apache.spark.rdd.RDD
import org.opencypher.spark.prototype.api.value.{CypherNode, CypherRelationship}
import org.opencypher.spark.legacy.impl.{SimplePattern, SimplePatternIds, StdPropertyGraph, SupportedQuery}

object RDDBenchmarks extends SupportedQueryBenchmarks[StdPropertyGraph] {

  def apply(query: SupportedQuery): Benchmark[StdPropertyGraph] = query match {
    case SimplePattern(startLabels, types, endLabels) =>
      simplePattern(query.toString, startLabels.head, types.head, endLabels.head)

    case SimplePatternIds(startLabels, types, endLabels) =>
      simplePatternIds(query.toString, startLabels.head, types.head, endLabels.head)

    case _ =>
      throw new IllegalArgumentException(s"No DataFrame implementation of $query")
  }

//  def nodeScanIdsSorted(label: String) = (graph: StdPropertyGraph) => {
//    val ids = graph.nodes.rdd.filter(CypherNode.labels(_).exists(_.contains(label))).map(CypherNode.id(_).map(_.v).get)
//    ids.sortBy(identity, ascending = false)
//  }

  def simplePatternIds(query: String, startLabel: String = "Group", relType: String = "ALLOWED_INHERIT", endLabel: String = "Company"): RDDBenchmark[Long] =
    new RDDBenchmark[Long](query) {
      override def name: String = "RDD       "

      def innerRun(graph: StdPropertyGraph): (RDD[Long], Long, Int) = {
        val nodes = graph.nodes.rdd
        val relationships = graph.relationships.rdd

        val startNodeIds = nodes.filter(CypherNode.labels(_).exists(_.contains(startLabel))).map(n => CypherNode.id(n).map(_.v).get -> CypherNode.id(n).map(_.v).get)
        val endNodeIds = nodes.filter(CypherNode.labels(_).exists(_.contains(endLabel))).map(n => CypherNode.id(n).map(_.v).get -> CypherNode.id(n).map(_.v).get)
        val relStartAndEndIds = relationships.filter(CypherRelationship.relationshipType(_).exists(_ == relType)).map(r => (CypherRelationship.startId(r).get.v, CypherRelationship.id(r).get.v -> CypherRelationship.endId(r).get.v))
        val keyOnEnd = startNodeIds.join(relStartAndEndIds).map {
          case (startId, (_, (rId, endId))) => (endId, rId -> startId)
        }
        val result = keyOnEnd.join(endNodeIds).map {
          case (endNodeId, ((rId, startNodeId), _)) => rId
        }

        val (count, checksum) = result.treeAggregate((0, 0))({
          case ((c, cs), relId) => (c + 1, cs ^ relId.hashCode())
        }, {
          case ((lCount, lChecksum), (rCount, rChecksum)) => (lCount + rCount, lChecksum ^ rChecksum)
        })

        (result, count, checksum)
      }
    }

  def simplePattern(query: String, startLabel: String = "Group", relType: String = "ALLOWED_INHERIT", endLabel: String = "Company"): RDDBenchmark[CypherRelationship] =
    new RDDBenchmark[CypherRelationship](query) {
      override def name: String = "RDD       "

      def innerRun(graph: StdPropertyGraph): (RDD[CypherRelationship], Long, Int) = {
        val nodes = graph.nodes.rdd
        val relationships = graph.relationships.rdd

        val startNodeIds = nodes.filter(CypherNode.labels(_).exists(_.contains(startLabel))).map(n => CypherNode.id(n).map(_.v).get -> CypherNode.id(n).map(_.v).get)
        val endNodeIds = nodes.filter(CypherNode.labels(_).exists(_.contains(endLabel))).map(n => CypherNode.id(n).map(_.v).get -> CypherNode.id(n).map(_.v).get)
        val relStartAndEndIds = relationships.filter(CypherRelationship.relationshipType(_).exists(_ == relType)).map(r => (CypherRelationship.startId(r).get.v, r -> CypherRelationship.endId(r).get.v))
        val keyOnEnd = startNodeIds.join(relStartAndEndIds).map {
          case (startId, (_, (r, endId))) => (endId, r -> startId)
        }
        val result = keyOnEnd.join(endNodeIds).map {
          case (endNodeId, ((r, startNodeId), _)) => r
        }

        val (count, checksum) = result.treeAggregate((0, 0))({
          case ((c, cs), rel) => (c +1, cs ^ CypherRelationship.id(rel).getOrElse(-1L).hashCode())
        }, {
          case ((lCount, lChecksum), (rCount, rChecksum)) => (lCount + rCount, lChecksum ^ rChecksum)
        })

        (result, count, checksum)
    }
  }
}

abstract class RDDBenchmark[T](query: String) extends Benchmark[StdPropertyGraph] with Serializable {
  override def name: String = "DataFrame"

  def numNodes(graph: StdPropertyGraph): Long =
    graph.nodes.count()

  def numRelationships(graph: StdPropertyGraph): Long =
    graph.relationships.count()

  override def plan(graph: StdPropertyGraph): String = "RDD"

  override def run(graph: StdPropertyGraph): Outcome = {
    val (rdd, count, checksum) = innerRun(graph)

    new Outcome {
      override val computeCount = count
      override lazy val computeChecksum = checksum
      override def usedCachedPlan: Boolean = false
    }
  }

  def innerRun(graph: StdPropertyGraph): (RDD[T], Long, Int)
}
