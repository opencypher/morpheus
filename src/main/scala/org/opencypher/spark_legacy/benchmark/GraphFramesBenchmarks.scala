/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.spark_legacy.benchmark

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.opencypher.spark_legacy.impl._

object GraphFramesBenchmarks
/*

  extends SupportedQueryBenchmarks[GraphFrame] {

  override def apply(query: SupportedQuery): Benchmark[GraphFrame] = query match {
    case SimplePatternIds(startLabels, types, endLabels) =>
      simplePatternIds(startLabels.head.toLowerCase, types.head, endLabels.head.toLowerCase)
    case MidPattern(startLabel, type1, midLabel, type2, endLabel) =>
      midPattern(startLabel.toLowerCase, type1, midLabel.toLowerCase, type2, endLabel.toLowerCase)
    case FixedLengthPattern(start, steps) =>
      fixedLengthPattern(start.toLowerCase, steps.map(p => p._1 -> p._2.toLowerCase))
    case _ =>
      throw new IllegalArgumentException(s"No GraphFrame implementation of $query")
  }

  def simplePatternIds(startLabel: String, relType: String, endLabel: String) = new GraphFrameBenchmark {
    override def innerRun(graphFrame: GraphFrame): DataFrame = {
      val dataFrame = graphFrame.find("(a)-[r]->(b)")
      // filter label and relationship type
      val filtered = dataFrame
        .filter(col(s"a.$startLabel"))
        .filter(col("r.type") === relType)
        .filter(col(s"b.$endLabel"))

      val result = filtered.select("r.id")

      result
    }
  }

  def midPattern(startLabel: String, type1: String, midLabel: String, type2: String, endLabel: String) = new GraphFrameBenchmark {
    override def innerRun(graphFrame: GraphFrame): DataFrame = {
      val df = graphFrame.find("(a)-[r1]->(b); (b)-[r2]->(c)")
      val filtered = df
        .filter(col(s"a.$startLabel"))
        .filter(col(s"r1.type") === type1)
        .filter(col(s"b.$midLabel"))
        .filter(col(s"r2.type") === type2)
        .filter(col(s"c.$endLabel"))

      val result = filtered.select("r1.id", "r2.id")

      result
    }
  }

  def buildMotif(steps: Seq[(Rel, String)]): String = {
    if (steps.head._1.isInstanceOf[In]) throw new IllegalArgumentException("Can not start with In step")
    steps.foldLeft(new StringBuilder(s"(n0)") -> 0) {
      case ((builder, step), (Out(_), _)) =>
        if (step > 0) builder.append(s"; (n$step)")
        val nextStep = step + 1
        val acc = builder.append(s"-[r$step]->(n$nextStep)")
        acc -> nextStep
      case ((builder, step), (In(_), _)) =>
        val nextStep = step + 1
        val acc = builder.append(s"; (n$nextStep)-[r$step]->")
        // not going to work if the first one is `In` ... let's not do that query
        if (step > 0) builder.append(s"(n$step)")
        acc -> nextStep
    }._1.toString
  }

  def fixedLengthPattern(startLabel: String, steps: Seq[(Rel, String)]) = new GraphFrameBenchmark {
    override def innerRun(graphFrame: GraphFrame): DataFrame = {
      var step = 0

      val motif = buildMotif(steps)

      step = 0
      var cols = Seq[Column]()

      val df = graphFrame.find(motif)
      val filtered = steps.foldLeft(df.filter(col(s"n$step.$startLabel"))) {
        case (frame, (rel, label)) =>
          cols = cols :+ col(s"r$step.id")
          step = step + 1
          frame
          .filter(col(s"r${step - 1}.type") === rel.relType)
          .filter(col(s"n$step.$label"))
      }

      val result = filtered.select(cols.reduce(_ + _))

      result
    }
  }
}

abstract class GraphFrameBenchmark extends Benchmark[GraphFrame] with Serializable {
  override def name: String = "GraphFrame"

  override def numNodes(graph: GraphFrame): Long = graph.vertices.count()

  override def numRelationships(graph: GraphFrame): Long = graph.edges.count()

  override def plan(graph: GraphFrame): String = innerRun(graph).queryExecution.toString

  override def run(graph: GraphFrame): Outcome = {
    val frame = innerRun(graph)
    val (count, checksum) = countAndChecksum(frame)

    new Outcome {
      override lazy val computeCount: Long = count
      override lazy val computeChecksum: Int = checksum
      override val usedCachedPlan: Boolean = false
    }
  }

  def innerRun(graphFrame: GraphFrame): DataFrame
}

*/
