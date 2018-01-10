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
import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.demo.Configuration.PrintLogicalPlan
import org.opencypher.caps.test.support.creation.caps.CAPSScanGraphFactory
import org.opencypher.tools.tck.api.CypherTCK

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class TCKCAPSTest {
  import TCKFixture._

  @TestFactory
  def run(): util.Collection[DynamicTest] = {

    // our interpretation of parameterized dynamic test in Scala with JUnit 5 ...
    val factories = Seq(
      CAPSScanGraphFactory
    )

    val tests = scenarios.filterNot { s =>
      ScenarioBlacklist.contains(s.toString())
    }

    factories.flatMap(factory => tests.map(dynamicTest(TCKGraph(factory, CAPSGraph.empty))).toList).asJavaCollection
  }

  @TestFactory
  def runBlacklistedTCKOnTestGraph(): util.Collection[DynamicTest] = {
    val tckGraph = TCKGraph.empty

    val tests = scenarios.filter { s =>
      ScenarioBlacklist.contains(s.toString())
    }

    tests.map { scenario =>
      DynamicTest.dynamicTest(
        scenario.toString,
        new Executable {
          override def execute(): Unit = {
            println(scenario)
            Try(scenario(tckGraph).execute()) match {
              case Success(_) =>
                throw new RuntimeException(s"A blacklisted scenario actually worked: $scenario")
              case Failure(_) =>
                ()
            }
          }
        }
      )
    }.asJavaCollection
  }

//  @TestFactory
  def runCustomScenario(): util.Collection[DynamicTest] = {
    val file = new File(getClass.getResource("CAPSTestFeature.feature").toURI)
    CypherTCK
      .parseFilesystemFeature(file)
      .scenarios
      .map(TCKFixture.dynamicTest(TCKGraph.empty))
      .asJavaCollection
  }

  //@TestFactory
  def runSingleScenario(): util.Collection[DynamicTest] = {
    val tckGraph = TCKGraph.empty
    PrintLogicalPlan.set()
    val name = "A simple pattern with one bound endpoint"
    scenarios.filter(s => s.name == name).map(dynamicTest(tckGraph)).asJavaCollection
  }
}
