package org.opencypher.spark.benchmark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.spark.api.value.CypherNode
import org.opencypher.spark.benchmark.AccessControlSchema.labelIndex
import org.opencypher.spark.impl.{MidPattern, SimplePatternIds, StdPropertyGraph, SupportedQuery}

object DataFrameBenchmarks extends SupportedQueryBenchmarks[SimpleDataFrameGraph] {

  def apply(query: SupportedQuery): Benchmark[SimpleDataFrameGraph] = query match {
    case SimplePatternIds(startLabels, types, endLabels) =>
      simplePatternIds(query.toString, startLabels.head, types.head, endLabels.head)
    case MidPattern(startLabel, type1, midLabel, type2, endLabel) =>
      midPattern(query.toString)(startLabel, type1, midLabel, type2, endLabel)
    case _ =>
      throw new IllegalArgumentException(s"No DataFrame implementation of $query")
  }

//  def nodeScanIdsSorted(label: String, sparkSession: SparkSession) = (graph: StdPropertyGraph) => {
//    import sparkSession.implicits._
//
//    val ids = graph.nodes.filter(CypherNode.labels(_).exists(_.contains(label))).map(CypherNode.id(_).map(_.v).get)
//    ids.sort(desc(ids.columns(0)))
//  }

  //MATCH (:Group)-[r:ALLOWED]->(:Company) RETURN id(r)
  def simplePatternIds(query: String, startLabel: String, relType: String, endLabel: String) = new DataFrameBenchmark(query) {

    override def innerRun(graph: SimpleDataFrameGraph) = {
      val startLabeled = graph.nodes(startLabel).as("startLabeled")
      val rels = graph.relationships(relType).as("rels")
      val endLabeled = graph.nodes(endLabel).as("endLabeled")

      val startJoined = startLabeled.join(rels, col("startLabeled.id") === col("rels.startId"))
      val endCol = startJoined.col("endId")
      val startJoinedShuffled = startJoined.sortWithinPartitions(endCol)
      val endJoined = startJoinedShuffled.join(endLabeled, endCol === col("endLabeled.id"))

      val result = endJoined.select(col("rels.id"))
      result
    }
  }

  def midPattern(query: String)(startLabel: String, type1: String, midLabel: String, type2: String, endLabel: String) = new DataFrameBenchmark(query) {
    override def innerRun(graph: SimpleDataFrameGraph) = {
      val startLabeled = graph.nodes(startLabel).as("startLabeled")
      val rel1 = graph.relationships(type1).as("rel1")
      val midLabeled = graph.nodes(midLabel).as("midLabeled")
      val rel2 = graph.relationships(type2).as("rel2")
      val endLabeled = graph.nodes(endLabel).as("endLabeled")

      val startJoined = startLabeled.join(rel1, col("startLabeled.id") === col("rel1.startId"))
      val endId1 = startJoined.col("rel1.endId")
      val startSorted = startJoined.sortWithinPartitions(endId1)
      val step1 = startSorted.join(midLabeled, endId1 === col("midLabeled.id"))
      val startId = step1.col("midLabeled.id")
      val step1Sorted = step1.sortWithinPartitions(startId)

      val step2 = step1Sorted.join(rel2, startId === col("rel2.startId"))
      val endId2 = step2.col("rel2.endId")
      val step2Sorted = step2.sortWithinPartitions(endId2)
      val endJoined = step2Sorted.join(endLabeled, endId2 === col("endLabeled.id"))

      val result = endJoined.select(col("rel1.id"), col("rel2.id"))
      result
    }
  }

  def largePattern(query: String)(startLabel: String, type1: String, midLabel1: String, type2: String, midLabel2: String, type3: String, endLabel: String) = new DataFrameBenchmark(query) {
    override def innerRun(graph: SimpleDataFrameGraph) = {
      val startLabeled = graph.nodes(startLabel).as("startLabeled")
      val rel1 = graph.relationships(type1).as("rel1")
      val midLabeled = graph.nodes(midLabel1).as("midLabeled")
      val rel2 = graph.relationships(type2).as("rel2")
      val endLabeled = graph.nodes(endLabel).as("endLabeled")

      val startJoined = startLabeled.join(rel1, col("startLabeled.id") === col("rel1.startId"))
      val endId1 = startJoined.col("rel1.endId")
      val startSorted = startJoined.sortWithinPartitions(endId1)
      val step1 = startSorted.join(midLabeled, endId1 === col("midLabeled.id"))
      val startId = step1.col("midLabeled.id")
      val step1Sorted = step1.sortWithinPartitions(startId)

      val step2 = step1Sorted.join(rel2, startId === col("rel2.startId"))
      val endId2 = step2.col("rel2.endId")
      val step2Sorted = step2.sortWithinPartitions(endId2)
      val endJoined = step2Sorted.join(endLabeled, endId2 === col("endLabeled.id"))

      val result = endJoined.select(col("rel1.id"), col("rel2.id"))
      result
    }
  }
}

abstract class DataFrameBenchmark(query: String) extends Benchmark[SimpleDataFrameGraph] with Serializable {
  override def name: String = "DataFrame "

  def numNodes(graph: SimpleDataFrameGraph): Long =
    graph.nodes.values.map(_.count()).sum

  def numRelationships(graph: SimpleDataFrameGraph): Long =
    graph.relationships.values.map(_.count()).sum

  override def plan(graph: SimpleDataFrameGraph): String =
      innerRun(graph).queryExecution.toString()

  override def run(graph: SimpleDataFrameGraph): Outcome = {
    val frame = innerRun(graph)
    val (count, checksum) = frame.rdd.treeAggregate((0l, 0))({
      case ((c, cs), rel) => (c + 1l, cs ^ rel.get(0).hashCode())
    }, {
      case ((lCount, lChecksum), (rCount, rChecksum)) => (lCount + rCount, lChecksum ^ rChecksum)
    })

    new Outcome {
      override lazy val computeCount = count
      override lazy val computeChecksum = checksum
      override def usedCachedPlan: Boolean = false
    }
  }

  def innerRun(graph: SimpleDataFrameGraph): DataFrame
}


/*

 n

 id(n), n.prop, n.prop2, n:X, properties(n), cast(n.prop, INTEGER)


 MATCH (:Group)-[r:ALLOWED]->(:Company) RETURN id(r)

 SELECT id FROM nodes
 SELECT id FROM nodes
 SELECT id, startId, endId FROM relationships

 join(all three)


 MATCH (:Group)-[r:ALLOWED]->(:Company) RETURN id(r)

 SELECT id FROM nodes_partitioned_by_id
 SELECT left_rel_ids, startId FROM relationships_partitioned_by_start
 SELECT right_rel_ids, endId FROM relationships_partitioned_by_end
 join(left_rel_ids, right_rel_ids)


 id -> :X, :Y, .prop1, .prop2

 1) One big flat table

 2) Multiple tables but with rules

    id (:X) -> .prop1 .prop2
    id (:Y) -> .prop4 .prop4
    id   -> .prop1

 */

case class SimpleDataFrameGraph(nodes: Map[String, DataFrame], relationships: Map[String, DataFrame])
