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

import org.junit.jupiter.api.{DynamicTest, TestFactory}
import org.opencypher.caps.TCKAdapterForCAPS.AsTckGraph
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSSession}
import org.opencypher.caps.demo.Configuration.PrintLogicalPlan
import org.opencypher.caps.test.support.testgraph.Neo4jTestGraph
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.CypherValue

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class CapsTCKTest {
  implicit val caps: CAPSSession = CAPSSession.local()
  def emptyGraph: CAPSGraph = CAPSGraph.empty
  val empty = Neo4jBackedTestGraph()

  @TestFactory
  def runTCKOnTestGraph(): util.Collection[DynamicTest] = {
    val dynamicTests = CypherTCK.allTckScenarios.filterNot(s => s.name.startsWith("Optionally matching self-loops") || s.featureName.startsWith("MatchingSelfRelationships")).map { scenario =>
      val name = scenario.toString
      val executable = scenario(empty)
      DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }

  @TestFactory
  def runSingleScenario(): util.Collection[DynamicTest] = {
    PrintLogicalPlan.set()
    val name = "A simple pattern with one bound endpoint"
    val dynamicTests = CypherTCK.allTckScenarios.filter(s => s.name == name).map { scenario =>
      val name = scenario.toString
      val executable = scenario(empty)
      DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }

  @TestFactory
  def runCustomOnNeo4j(): util.Collection[DynamicTest] = {
    PrintLogicalPlan.set()
    val file = new File(getClass.getResource("CAPSTestFeature.feature").toURI)
    val dynamicTests = CypherTCK.parseFilesystemFeature(file).scenarios.map { scenario =>
      val name = scenario.toString
      val executable = scenario(emptyGraph)
      DynamicTest.dynamicTest(name, executable)
    }
    dynamicTests.asJavaCollection
  }

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
