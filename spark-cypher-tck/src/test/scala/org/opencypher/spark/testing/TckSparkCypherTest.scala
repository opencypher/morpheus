/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.testing

import java.io.File

import org.opencypher.okapi.tck.test.Tags.{BlackList, WhiteList}
import org.opencypher.okapi.tck.test.{ScenariosFor, TCKGraph}
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.testing.support.creation.caps.{CAPSScanGraphFactory, CAPSTestGraphFactory}
import org.opencypher.tools.tck.api.CypherTCK
import org.scalatest.Tag
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.util.{Failure, Success, Try}

class TckSparkCypherTest extends CAPSTestSuite {

  object TckCapsTag extends Tag("TckSparkCypher")

  // Defines the graphs to run on
  private val factories = Table(
    ("factory","additional_blacklist"),
    (CAPSScanGraphFactory, Set.empty[String])
  )

  private val defaultFactory: CAPSTestGraphFactory = CAPSScanGraphFactory

  private val blacklistFile = getClass.getResource("/scenario_blacklist").getFile
  private val scenarios = ScenariosFor(blacklistFile)

  // white list tests are run on all factories
  forAll(factories) { (factory, additional_blacklist) =>
    forAll(scenarios.whiteList) { scenario =>
      if (!additional_blacklist.contains(scenario.toString)) {
        test(s"[${factory.name}, ${WhiteList.name}] $scenario", WhiteList, TckCapsTag, Tag(factory.name)) {
          scenario(TCKGraph(factory, CAPSGraph.empty)).execute()
        }
      }
    }
  }

  // black list tests are run on default factory
  forAll(scenarios.blackList) { scenario =>
    test(s"[${defaultFactory.name}, ${BlackList.name}] $scenario", BlackList, TckCapsTag) {
      val tckGraph = TCKGraph(defaultFactory, CAPSGraph.empty)

      Try(scenario(tckGraph).execute()) match {
        case Success(_) =>
          throw new RuntimeException(s"A blacklisted scenario actually worked: $scenario")
        case Failure(_) =>
          ()
      }
    }
  }

  ignore("run Custom Scenario") {
    val file = new File(getClass.getResource("CustomTest.feature").toURI)

    CypherTCK
      .parseFilesystemFeature(file)
      .scenarios
      .foreach(scenario => scenario(TCKGraph(defaultFactory, CAPSGraph.empty)).execute())
  }

  it("run Single Scenario") {
    scenarios.get("`toBoolean()` on invalid types")
      .foreach(scenario => scenario(TCKGraph(defaultFactory, CAPSGraph.empty)).execute())
  }
}
