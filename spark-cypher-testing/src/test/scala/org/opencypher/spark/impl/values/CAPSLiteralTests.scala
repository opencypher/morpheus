/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.impl.values

import claimant.Claim
import org.opencypher.okapi.api.value.CypherValue.Format._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, _}
import org.opencypher.okapi.api.value.GenCypherValue._
import org.opencypher.spark.impl.acceptance.ScanGraphInit
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalacheck.Gen.const
import org.scalacheck.{Gen, Prop}
import org.scalatestplus.scalacheck.Checkers

class CAPSLiteralTests extends CAPSTestSuite with Checkers with ScanGraphInit {

  val supportedLiteral: Gen[CypherValue] = Gen.oneOf(
    homogeneousScalarList, propertyMap, string, boolean, integer, float, const(CypherNull)
  )

  it("round trip for supported literals") {
    check(Prop.forAll(supportedLiteral) { v: CypherValue =>
      val query = s"RETURN ${v.toCypherString} AS result"
      val result = caps.cypher(query).records.collect.toList
      val expected = List(CypherMap("result" -> v))
      Claim(result == expected)
    }, minSuccessful(10))
  }

  it("round trip for nodes") {
    check(Prop.forAll(node) { n: CypherNode[CypherInteger] =>
      val graph = initGraph(s"CREATE ${n.toCypherString}")
      val query = s"MATCH (n) RETURN n"
      val result = TestNode(graph.cypher(query).records.collect.head("n").cast[CypherNode[_]])
      Claim(result == n)
    }, minSuccessful(10))
  }

  // TODO: Diagnose and fix error "AnalysisException: Reference 'node_L __ BOOLEAN' is ambiguous, could be: node_L __ BOOLEAN, node_L __ BOOLEAN.;"
  ignore("round trip for relationships") {
    check(Prop.forAll(nodeRelNodePattern()) { p: NodeRelNodePattern[_] =>
      val graph = initGraph(p.toCreateQuery)
      val query = s"MATCH ()-[r]->() RETURN r"
      val result = TestRelationship(graph.cypher(query).records.collect.head("r").cast[CypherRelationship[_]])
      Claim(result == p.relationship)
    }, minSuccessful(1000))
  }

}
