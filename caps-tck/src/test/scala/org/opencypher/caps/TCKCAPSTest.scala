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

import java.util

import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{DynamicTest, TestFactory}
import org.opencypher.caps.demo.Configuration.PrintLogicalPlan

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class TCKCAPSTest {
  import TCKFixture._

  val empty = Neo4jBackedTestGraph()

  @TestFactory
  def run(): util.Collection[DynamicTest] = {
    val tests = scenarios.filterNot { s =>
      ScenarioBlacklist.contains(s.toString())
    }

    tests.map(dynamicTest(empty)).asJavaCollection
  }

  @TestFactory
  def runBlacklistedTCKOnTestGraph(): util.Collection[DynamicTest] = {
    val tests = scenarios.filter { s =>
      ScenarioBlacklist.contains(s.toString())
    }

    tests.map { scenario =>
      DynamicTest.dynamicTest(
        scenario.toString,
        new Executable {
          override def execute(): Unit = {
            println(scenario)
            Try(scenario(empty).execute()) match {
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

  //@TestFactory
  def runSingleScenario(): util.Collection[DynamicTest] = {
    PrintLogicalPlan.set()
    val name = "A simple pattern with one bound endpoint"
    scenarios.filter(s => s.name == name).map(dynamicTest(empty)).asJavaCollection
  }
}
