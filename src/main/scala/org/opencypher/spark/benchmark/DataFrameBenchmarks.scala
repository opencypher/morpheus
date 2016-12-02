package org.opencypher.spark.benchmark

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.opencypher.spark.impl._

object DataFrameBenchmarks extends SupportedQueryBenchmarks[SimpleDataFrameGraph] {

  def apply(query: SupportedQuery): Benchmark[SimpleDataFrameGraph] = query match {
    case SimplePatternIds(startLabels, types, endLabels) =>
      simplePatternIds(startLabels.head, types.head, endLabels.head)
    case MidPattern(startLabel, type1, midLabel, type2, endLabel) =>
      midPattern(startLabel, type1, midLabel, type2, endLabel)
    case FixedLengthPattern(start, steps) =>
      fixedLengthPattern(start, steps)
    case _ =>
      throw new IllegalArgumentException(s"No DataFrame implementation of $query")
  }

//  def nodeScanIdsSorted(label: String, sparkSession: SparkSession) = (graph: StdPropertyGraph) => {
//    import sparkSession.implicits._
//
//    val ids = graph.nodes.filter(CypherNode.labels(_).exists(_.contains(label))).map(CypherNode.id(_).map(_.v).get)
//    ids.sort(desc(ids.columns(0)))
//  }

  def simplePatternIds(startLabel: String, relType: String, endLabel: String) = new DataFrameBenchmark {

    override def innerRun(graph: SimpleDataFrameGraph) = {
      val startLabeled = graph.nodes(startLabel).as("startLabeled")
      val rels = graph.relationshipsByStartId(relType).as("rels")
      val endLabeled = graph.nodes(endLabel).as("endLabeled")

      val startJoined = startLabeled.join(rels, col("startLabeled.id") === col("rels.startId"))
      val endCol = startJoined.col("endId")
      val endJoined = startJoined.join(endLabeled, endCol === col("endLabeled.id"))

      val result = endJoined.select(col("rels.id"))
      result
    }
  }

  def midPattern(startLabel: String, type1: String, midLabel: String, type2: String, endLabel: String) = new DataFrameBenchmark {
    override def innerRun(graph: SimpleDataFrameGraph) = {
      val startLabeled = graph.nodes(startLabel).as("startLabeled")
      val rel1 = graph.relationshipsByStartId(type1).as("rel1")
      val midLabeled = graph.nodes(midLabel).as("midLabeled")
      val rel2 = graph.relationshipsByStartId(type2).as("rel2")
      val endLabeled = graph.nodes(endLabel).as("endLabeled")

      val startJoined = startLabeled.join(rel1, col("startLabeled.id") === col("rel1.startId"))
      val endId1 = startJoined.col("rel1.endId")
      val step1 = startJoined.join(midLabeled, endId1 === col("midLabeled.id"))
      val startId = step1.col("midLabeled.id")

      val step2 = step1.join(rel2, startId === col("rel2.startId"))
      val endId2 = step2.col("rel2.endId")
      val endJoined = step2.join(endLabeled, endId2 === col("endLabeled.id"))

      val result = endJoined.select(col("rel1.id"), col("rel2.id"))
      result
    }
  }

  def fixedLengthPattern(startLabel: String, steps: Seq[(Rel, String)]) = new DataFrameBenchmark {
    override def innerRun(graph: SimpleDataFrameGraph) = {
      var step = 1
      var current = graph.nodes(startLabel).as(s"n$step")
      var cols = Seq[Column]()

      steps.foreach {
        case (rel, label) =>
          val rels = graph.relationshipsForRel(rel).as(s"r$step")
          val joined = current.join(rels, col(s"n$step.id") === rel.source(s"r$step"))
          val nextStep = step + 1
          val other = graph.nodes(label).as(s"n$nextStep")
          current = joined.join(other, rel.target(s"r$step") === col(s"n$nextStep.id"))
          cols = cols :+ col(s"r$step.id")
          step = nextStep
      }

      val result = current.select(cols.reduce(_ + _))
      result
    }
  }
}

abstract class DataFrameBenchmark extends Benchmark[SimpleDataFrameGraph] with Serializable {
  override def name: String = "DataFrame "

  def numNodes(graph: SimpleDataFrameGraph): Long =
    graph.nodes.values.map(_.count()).sum // not correct if nodes have > 1 label

  def numRelationships(graph: SimpleDataFrameGraph): Long =
    graph.relationships.values.map(_._1.count()).sum

  override def plan(graph: SimpleDataFrameGraph): String =
      innerRun(graph).queryExecution.toString

  override def run(graph: SimpleDataFrameGraph): Outcome = {
    val frame = innerRun(graph)
    val (count, checksum) = countAndChecksum(frame)

    new Outcome {
      override lazy val computeCount = count
      override lazy val computeChecksum = checksum
      override val usedCachedPlan: Boolean = false
    }
  }

  def innerRun(graph: SimpleDataFrameGraph): DataFrame
}

case class SimpleDataFrameGraph(nodes: Map[String, DataFrame], relationships: Map[String, (DataFrame, DataFrame)]) {
  def relationshipsByStartId(name: String): DataFrame = relationships(name)._1
  def relationshipsByEndId(name: String): DataFrame = relationships(name)._2
  def relationshipsForRel(rel: Rel): DataFrame = rel match {
    case Out(name) => relationshipsByStartId(name)
    case In(name) => relationshipsByEndId(name)
  }
}
