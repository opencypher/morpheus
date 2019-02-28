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
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.impl.acceptance.ScanGraphInit
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalacheck.Gen.const
import org.scalacheck.{Gen, Prop}
import org.scalatest.prop.Checkers

class CAPSLiteralTests extends CAPSTestSuite with Checkers with ScanGraphInit {

  val supportedLiteral: Gen[CypherValue] = Gen.oneOf(
    homogeneousScalarListGenerator, map, string, boolean, integer, float, const(CypherNull)
  )

  // TODO: Remove once map bug in test below is fixed
  it("round trip for scalar literals") {
    check(Prop.forAll(scalarOrNull) { v: CypherValue =>
      val query = s"RETURN ${v.toCypherString} AS result"
      val result = caps.cypher(query).records.toMaps
      val expected = Bag(CypherMap("result" -> v))
      Claim(result == expected)
    }, minSuccessful(100))
  }

  // TODO: Fix empty map bug
  ignore("round trip for supported literals") {
    check(Prop.forAll(supportedLiteral) { v: CypherValue =>
      val query = s"RETURN ${v.toCypherString} AS result"
      val result = caps.cypher(query).records.toMaps
      val expected = Bag(CypherMap("result" -> v))
      Claim(result == expected)
    }, minSuccessful(100))
  }

  // TODO: Fix "SchemaException: Labels must be non-empty" bug
  ignore("round trip for nodes") {
    check(Prop.forAll(node) { n: CypherNode[CypherInteger] =>
      val given = initGraph(s"CREATE ${n.toCypherString}")
      val query = s"MATCH (n) RETURN n"
      val result = TestNode(given.cypher(query).records.collect.head("n").cast[CypherNode[_]])
      Claim(result == n)
    }, minSuccessful(100))
  }


  // TODO: Diagnose error "NotImplementedException was thrown during property evaluation: Mapping of CypherType ANY to Spark type"
  // TODO: Diagnose error "SchemaException was thrown during property evaluation: Labels must be non-empty"
  ignore("round trip for relationships") {
    check(Prop.forAll(nodeRelNodePattern()) { p: NodeRelNodePattern[_] =>
      val given = initGraph(p.toCreateQuery)
      val query = s"MATCH ()-[r]->() RETURN r"
      val result = TestRelationship(given.cypher(query).records.collect.head("r").cast[CypherRelationship[_]])
      Claim(result == p.relationship)
    }, minSuccessful(100))
  }

}
