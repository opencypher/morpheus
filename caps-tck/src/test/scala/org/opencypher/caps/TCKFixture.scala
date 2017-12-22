/*
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
package org.opencypher.caps

import java.io.File
import java.util

import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{DynamicTest, TestFactory}
import org.opencypher.caps.TCKAdapterForCAPS.AsTckGraph
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.demo.Configuration.PrintLogicalPlan
import org.opencypher.caps.test.support.testgraph.Neo4jTestGraph
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.CypherValue

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.{Failure, Success, Try}

// this is an object with a val because we can only load the
// scenarios _once_ due to a bug in the TCK API
object TCKFixture {
  val scenarios: Seq[Scenario] = CypherTCK.allTckScenarios

  def dynamicTest(graph: Graph)(scenario: Scenario): DynamicTest =
    DynamicTest.dynamicTest(scenario.toString, new Executable {
      override def execute(): Unit = {
        println(scenario)
        scenario(graph).execute()
      }
    })

  implicit val caps: CAPSSession = CAPSSession.local()
}

case class Neo4jBackedTestGraph(implicit caps: CAPSSession) extends Graph {
  private val neo4jGraph = Neo4jTestGraph("RETURN 1")

  override def execute(query: String, params: Map[String, CypherValue], queryType: QueryType): (Graph, Result) = {
    queryType match {
      case InitQuery =>
        // we use an embedded Neo4j for this
        // this works because there is never more than one init query
        neo4jGraph.inputGraph.execute("MATCH (a) DETACH DELETE a")
        neo4jGraph.inputGraph.execute(query)
        val capsGraph = Try(neo4jGraph.capsGraph) match {
          case Success(g) =>
            g
          case Failure(err) =>
            CAPSGraph.empty
        }

        AsTckGraph(capsGraph) -> CypherValueRecords.empty
      case _ =>
        ???
    }
  }
}

object ScenarioBlacklist {
  private lazy val blacklist: Set[String] = {
    val blacklistIter = Source.fromFile(getClass.getResource("scenario_blacklist").toURI).getLines().toSeq
    val blacklistSet = blacklistIter.toSet

    lazy val errorMessage = s"Blacklist contains duplicate scenarios ${blacklistIter.groupBy(identity).filter(_._2.lengthCompare(1) > 0).keys.mkString("\n")}"
    assert(blacklistIter.size == blacklistSet.size, errorMessage)
    blacklistSet
  }

  def contains(name: String): Boolean = blacklist.contains(name)
}
