/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.ir.test.support.creation.propertygraph.TestPropertyGraphFactory
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.support.creation.caps.CAPSTestGraphFactory

abstract class AcceptanceTest
  extends CAPSTestSuite
    with AggregationBehaviour
    with BoundedVarExpandBehaviour
    with ExpandIntoBehaviour
    with ExpressionBehaviour
    with FunctionsBehaviour
    with MatchBehaviour
    with MultipleGraphBehaviour
    with OptionalMatchBehaviour
    with PredicateBehaviour
    with ReturnBehaviour
    with WithBehaviour
    with UnwindBehaviour
    with CatalogDDLBehaviour {

  def capsGraphFactory: CAPSTestGraphFactory

  val initGraph: String => CAPSGraph = (createQuery) =>
    capsGraphFactory(TestPropertyGraphFactory(createQuery)).asCaps

  describe("using " + capsGraphFactory.name) {
    describe("AggregationBehaviour") {
      it should behave like aggregationBehaviour(initGraph)
    }

    describe("BoundedVarExpandBehaviour") {
      it should behave like boundedVarExpandBehaviour(initGraph)
    }

    describe("CatalogDDLBehaviour") {
      it should behave like catalogDDLBehaviour(initGraph)
    }

    describe("ExpandIntoBehaviour") {
      it should behave like expandIntoBehaviour(initGraph)
    }

    describe("ExpressionBehaviour") {
      it should behave like expressionBehaviour(initGraph)
    }

    describe("FunctionsBehaviour") {
      it should behave like functionsBehaviour(initGraph)
    }

    describe("MatchBehaviour") {
      it should behave like matchBehaviour(initGraph)
    }

    describe("MultigraphProjectionBehaviour") {
      it should behave like multipleGraphBehaviour(initGraph)
    }

    describe("OptionalMatchBehaviour") {
      it should behave like optionalMatchBehaviour(initGraph)
    }

    describe("PredicateBehaviour") {
      it should behave like predicateBehaviour(initGraph)
    }

    describe("ReturnBehaviour") {
      it should behave like returnBehaviour(initGraph)
    }

    describe("WithBehaviour") {
      it should behave like withBehaviour(initGraph)
    }

    describe("UnwindBehaviour") {
      it should behave like unwindBehaviour(initGraph)
    }
  }
}
