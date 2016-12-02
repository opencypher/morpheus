package org.opencypher.spark.benchmark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}
import org.opencypher.spark.impl._

object TripletBenchmarks extends SupportedQueryBenchmarks[TripletGraph] {
  override def apply(query: SupportedQuery) = query match {
    case SimplePatternIds(startLabels, types, endLabels) =>
      simplePatternIds(startLabels.head, types.head, endLabels.head.toLowerCase)
    case FixedLengthPattern(start, steps) =>
      fixedLength(start, steps.map(p => p._1 -> p._2.toLowerCase))
  }

  private def simplePatternIds(startLabel: String, typ: String, endLabel: String) = new TripletBenchmark {
    override def innerRun(graph: TripletGraph) = {
      val nodes = graph.nodes(startLabel).as("n")
      val triplets = graph.tripletByStartId(typ).as("r")
      val expanded = nodes.join(triplets, nodes.col("n.id") === col("r.startId"))
      val filtered = expanded.filter(col(s"r.$endLabel"))
      val result = filtered.select("r.id")

      result
    }
  }

  private def fixedLength(startLabel: String, steps: Seq[(Rel, String)]) = new TripletBenchmark {
    override def innerRun(graph: TripletGraph) = {
      if (steps.isEmpty) graph.nodes(startLabel)
      else {
        var current = graph.nodes(startLabel).as(s"n")
        var cols = Seq[Column]()
        var prev = current.col(s"n.id")
        var step = 1

        steps.foreach {
          case (rel, label) =>
            val triplet = graph.tripletForRel(rel).as(s"r$step")
            val joined = current.join(triplet, prev === rel.source(s"r$step")).drop(prev)
            current = joined.filter(col(s"r$step.$label"))
            prev = rel.target(s"r$step")
            cols = cols :+ col(s"r$step.id")
            step = step + 1
        }

        val result = current.select(cols.reduce(_ + _))
        result
      }
    }
  }
}

abstract class TripletBenchmark extends Benchmark[TripletGraph] {
  override def name: String = "Triplet   "

  override def plan(graph: TripletGraph): String = {
    innerRun(graph).queryExecution.toString
  }

  override def run(graph: TripletGraph): Outcome = {
    val frame = innerRun(graph)

    val (count, checksum) = countAndChecksum(frame)

    new Outcome {
      override lazy val computeCount = count
      override lazy val computeChecksum = checksum
      override val usedCachedPlan: Boolean = false
    }
  }

  override def numNodes(graph: TripletGraph): Long =
    graph.nodes.values.map(_.count()).sum // not correct if nodes have > 1 label

  override def numRelationships(graph: TripletGraph): Long =
    graph.triplets.values.map(_._1.count()).sum

  def innerRun(graph: TripletGraph): DataFrame
}

case class TripletGraph(nodes: Map[String, DataFrame], triplets: Map[String, (DataFrame, DataFrame)]) {
  def tripletByStartId(relType: String): DataFrame = triplets(relType)._1
  def tripletByEndId(relType: String): DataFrame = triplets(relType)._2
  def tripletForRel(rel: Rel): DataFrame = rel match {
    case Out(name) => tripletByStartId(name)
    case In(name) => tripletByEndId(name)
  }
}
