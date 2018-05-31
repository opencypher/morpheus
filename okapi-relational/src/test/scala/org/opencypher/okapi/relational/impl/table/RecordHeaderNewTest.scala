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
package org.opencypher.okapi.relational.impl.table

import org.opencypher.okapi.api.types.{CTBoolean, CTNode, CTString}
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.ir.api.expr.{HasLabel, Property, Var}
import org.scalatest.{FunSpec, Matchers}

import org.opencypher.okapi.ir.test.support.MatchHelper._

class RecordHeaderNewTest extends FunSpec with Matchers {

  val n = Var("n")(CTNode)
  val m = Var("m")(CTNode)
  val o = Var("o")(CTNode)

  val label1 = HasLabel(n, Label("A"))(CTBoolean)
  val label2 = HasLabel(n, Label("B"))(CTBoolean)
  val prop = Property(n, PropertyKey("foo"))(CTString)
  val exprs = Set(n, label1, label2, prop)

  it("can add a node") {
    val header = RecordHeaderNew.empty
      .withExpr(n)
      .withExpr(label1)
      .withExpr(label2)
      .withExpr(prop)

    header.ownedBy(n) should equal(exprs)
  }

  it("can add a node alias") {
    val header = RecordHeaderNew.empty
      .withExpr(n)
      .withExpr(label1)
      .withExpr(label2)
      .withExpr(prop)

    val m = Var("m")(CTNode)
    val withAlias = header.withAlias(m, n)
    val exprsWithNewOwner = exprs.map(_.withOwner(m))

    withAlias.ownedBy(n) should equalWithTracing(exprs)
    withAlias.ownedBy(m) should equalWithTracing(exprsWithNewOwner)
  }

  it("add alias for non-entity expr") {
    val header = RecordHeaderNew.empty
      .withExpr(n)
      .withExpr(label1)
      .withExpr(label2)
      .withExpr(prop)

    val withAlias1 = header.withAlias(m, prop)
    val withAlias2 = withAlias1.withAlias(o, m)


    withAlias2.column(o) should equalWithTracing(withAlias2.column(prop))
    withAlias2.column(m) should equalWithTracing(withAlias2.column(prop))
    withAlias2.ownedBy(n) should equalWithTracing(exprs)
  }

}
