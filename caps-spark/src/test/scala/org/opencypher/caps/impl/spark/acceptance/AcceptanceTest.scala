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
package org.opencypher.caps.impl.spark.acceptance

import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.support.creation.caps.CAPSGraphFactory
import org.opencypher.caps.test.support.creation.propertygraph.CAPSPropertyGraphFactory

abstract class AcceptanceTest
    extends CAPSTestSuite
    with AggregationBehaviour
    with BoundedVarExpandBehaviour
    with ExpandIntoBehaviour
    with ExpressionBehaviour
    with FunctionsBehaviour
    with MatchBehaviour
    with MultigraphProjectionBehaviour
    with OptionalMatchBehaviour
    with PredicateBehaviour
    with ReturnBehaviour
    with WithBehaviour
    with UnwindBehaviour {

  def capsGraphFactory: CAPSGraphFactory

  val initGraph: String => CAPSGraph = (createQuery) => capsGraphFactory(CAPSPropertyGraphFactory(createQuery))

  describe("using " + capsGraphFactory.name) {
    describe("AggregationBehaviour") {
      it should behave like aggregationBehaviour(initGraph)
    }

    describe("BoundedVarExpandBehaviour") {
      it should behave like boundedVarExpandBehaviour(initGraph)
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
      it should behave like multigraphProjectionBehaviour(initGraph)
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
