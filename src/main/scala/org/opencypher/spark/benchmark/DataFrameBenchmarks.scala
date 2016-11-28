package org.opencypher.spark.benchmark

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.opencypher.spark.impl._

object DataFrameBenchmarks extends SupportedQueryBenchmarks[SimpleDataFrameGraph] {

  def apply(query: SupportedQuery): Benchmark[SimpleDataFrameGraph] = query match {
    case SimplePatternIds(startLabels, types, endLabels) =>
      simplePatternIds(query.toString, startLabels.head, types.head, endLabels.head)
    case MidPattern(startLabel, type1, midLabel, type2, endLabel) =>
      midPattern(query.toString)(startLabel, type1, midLabel, type2, endLabel)
    case FixedLengthPattern(start, steps) =>
      fixedLengthPattern(query.toString)(start, steps)
    case _ =>
      throw new IllegalArgumentException(s"No DataFrame implementation of $query")
  }

//  def nodeScanIdsSorted(label: String, sparkSession: SparkSession) = (graph: StdPropertyGraph) => {
//    import sparkSession.implicits._
//
//    val ids = graph.nodes.filter(CypherNode.labels(_).exists(_.contains(label))).map(CypherNode.id(_).map(_.v).get)
//    ids.sort(desc(ids.columns(0)))
//  }

  def simplePatternIds(query: String, startLabel: String, relType: String, endLabel: String) = new DataFrameBenchmark(query) {

    override def innerRun(graph: SimpleDataFrameGraph) = {
      val startLabeled = graph.nodes(startLabel).as("startLabeled")
      val rels = graph.relationshipsByStartId(relType).as("rels")
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
      val rel1 = graph.relationshipsByStartId(type1).as("rel1")
      val midLabeled = graph.nodes(midLabel).as("midLabeled")
      val rel2 = graph.relationshipsByStartId(type2).as("rel2")
      val endLabeled = graph.nodes(endLabel).as("endLabeled")

      val startJoined = startLabeled.join(rel1, col("startLabeled.id") === col("rel1.startId"))
      val endId1 = startJoined.col("rel1.endId")
      val startSorted = startJoined.sortWithinPartitions(endId1)
      val step1 = startSorted.join(midLabeled, endId1 === col("midLabeled.id"))
      val startId = step1.col("midLabeled.id")
      // val step1Sorted = step1.sortWithinPartitions(startId)

      val step2 = step1.join(rel2, startId === col("rel2.startId"))
      val endId2 = step2.col("rel2.endId")
      val step2Sorted = step2.sortWithinPartitions(endId2)
      val endJoined = step2Sorted.join(endLabeled, endId2 === col("endLabeled.id"))

      val result = endJoined.select(col("rel1.id"), col("rel2.id"))
      result
    }
  }

  def fixedLengthPattern(query: String)(startLabel: String, steps: Seq[(Rel, String)]) = new DataFrameBenchmark(query) {
    override def innerRun(graph: SimpleDataFrameGraph) = {
      var step = 1
      var current = graph.nodes(startLabel).as(s"n$step")
      var cols = Seq[Column]()

      steps.foreach {
        case (rel, label) =>
          val rels = graph.relationshipsForRel(rel).as(s"r$step")
          val joined = current.join(rels, col(s"n$step.id") === rel.source(s"r$step"))
          val sorted = joined.sortWithinPartitions(rel.target(s"r$step"))
          val nextStep = step + 1
          val other = graph.nodes(label).as(s"n$nextStep")
          val filtered = sorted.join(other, rel.target(s"r$step") === col(s"n$nextStep.id"))
          current = filtered.sortWithinPartitions(col(s"n$nextStep.id"))
          cols = cols :+ col(s"r$step.id")
          step = nextStep
      }

      val result = current.select(cols.reduce(_ + _))
      result
    }
  }
}

abstract class DataFrameBenchmark(query: String) extends Benchmark[SimpleDataFrameGraph] with Serializable {
  override def name: String = "DataFrame "

  def numNodes(graph: SimpleDataFrameGraph): Long =
    graph.nodes.values.map(_.count()).sum

  def numRelationships(graph: SimpleDataFrameGraph): Long =
    graph.relationships.values.map(_._1.count()).sum

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

case class SimpleDataFrameGraph(nodes: Map[String, DataFrame], relationships: Map[String, (DataFrame, DataFrame)]) {
  def relationshipsByStartId(name: String): DataFrame = relationships(name)._1
  def relationshipsByEndId(name: String): DataFrame = relationships(name)._2
  def relationshipsForRel(rel: Rel) = rel match {
    case Out(name) => relationshipsByStartId(name)
    case In(name) => relationshipsByEndId(name)
  }
}
