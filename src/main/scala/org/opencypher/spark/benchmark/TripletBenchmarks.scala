package org.opencypher.spark.benchmark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}
import org.opencypher.spark.impl._

object TripletBenchmarks extends SupportedQueryBenchmarks[TripletGraph] {
  override def apply(query: SupportedQuery) = query match {
    case FixedLengthPattern(start, steps) =>
      fixedLength(start, steps.map(p => p._1 -> p._2.toLowerCase))
  }

  /**
    * Requires labels to be lower case
    */
  def fixedLength(startLabel: String, steps: Seq[(Rel, String)]) = new Benchmark[TripletGraph] {
    override def name: String = "Triplet   "

    override def plan(graph: TripletGraph): String = {
      val frame = if (steps.isEmpty) graph.nodes(startLabel)
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

      frame.queryExecution.toString
    }

    override def run(graph: TripletGraph): Outcome = {
      val frame = if (steps.isEmpty) graph.nodes(startLabel)
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

//      frame.show()
//      frame.explain()

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
  }
}


case class TripletGraph(nodes: Map[String, DataFrame], triplets: Map[String, (DataFrame, DataFrame)]) {
  def tripletByStartId(relType: String): DataFrame = triplets(relType)._1
  def tripletByEndId(relType: String): DataFrame = triplets(relType)._2
  def tripletForRel(rel: Rel): DataFrame = rel match {
    case Out(name) => tripletByStartId(name)
    case In(name) => tripletByEndId(name)
  }
}
