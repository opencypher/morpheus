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
package org.opencypher.okapi.relational.api.schema

import org.opencypher.okapi.api.schema.{PropertyKeys, Schema}
import org.opencypher.okapi.api.types.{CTBoolean, CTNode}
import org.opencypher.okapi.ir.api.expr.{HasLabel, Property, Var}
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.impl.table.RecordHeaderNew
import org.scalatest.{FunSpec, Matchers}

class RelationalSchemaTest extends FunSpec with Matchers {

  it("creates a header for given node") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set("A", "B"), PropertyKeys("foo" -> CTBoolean))

    val n = Var("n")(CTNode(Set("A", "B")))

    schema.headerForNode(n) should equal(RecordHeaderNew.empty
      .withExpr(n)
      .withExpr(HasLabel(n, Label("A"))(CTBoolean))
      .withExpr(HasLabel(n, Label("B"))(CTBoolean))
      .withExpr(Property(n, PropertyKey("foo"))(CTBoolean)))
  }

  it("creates a header for given node with implied labels") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set("A"), PropertyKeys("foo" -> CTBoolean))
      .withNodePropertyKeys(Set("A", "B"), PropertyKeys("bar" -> CTBoolean))

    val n = Var("n")(CTNode(Set("A")))

    schema.headerForNode(n) should equal(RecordHeaderNew.empty
      .withExpr(n)
      .withExpr(HasLabel(n, Label("A"))(CTBoolean))
      .withExpr(HasLabel(n, Label("B"))(CTBoolean))
      .withExpr(Property(n, PropertyKey("foo"))(CTBoolean))
      .withExpr(Property(n, PropertyKey("bar"))(CTBoolean)))
  }
}
