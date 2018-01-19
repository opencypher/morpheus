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
 */
package org.opencypher.caps.logical.impl

import java.net.URI

import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTBoolean, CTNode, CTRelationship}
import org.opencypher.caps.ir.api.{IRNamedGraph, RelType}
import org.opencypher.caps.ir.api.block.{FieldsAndGraphs, ProjectedFieldsOf}
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.api.pattern.Pattern
import org.opencypher.caps.ir.impl.IrTestSuite
import org.opencypher.caps.ir.test._

class SolvedQueryModelTest extends IrTestSuite {

  implicit val uri = URI.create("test")

  test("add graphs") {
    val s = SolvedQueryModel.empty.withGraph('foo)

    s.graphs should equal(Set(IRNamedGraph("foo", Schema.empty)))
  }

  test("add fields") {
    val s = SolvedQueryModel.empty.withField('a).withFields('b, 'c)

    s.fields should equal(Set('a, 'b, 'c).map(toField))
  }

  test("contains a block") {
    val block = matchBlock(Pattern.empty.withEntity('a).withEntity('b).withEntity('c))
    val s = SolvedQueryModel.empty.withField('a).withFields('b, 'c)

    s.contains(block) shouldBe true
  }

  test("contains several blocks") {
    val block1 = matchBlock(Pattern.empty.withEntity('a -> CTNode))
    val block2 = matchBlock(Pattern.empty.withEntity('b -> CTNode))
    val binds: FieldsAndGraphs[Expr] = FieldsAndGraphs(Map(toField('c) -> Equals('a, 'b)(CTBoolean)), Set('foo))
    val block3 = project(binds)
    val block4 = project(ProjectedFieldsOf(toField('d) -> Equals('c, 'b)(CTBoolean)))
    val block5 = project(FieldsAndGraphs(Map.empty, Set('bar)))

    val s = SolvedQueryModel.empty.withField('a).withFields('b, 'c).withGraph('foo)

    s.contains(block1) shouldBe true
    s.contains(block1, block2) shouldBe true
    s.contains(block1, block2, block3) shouldBe true
    s.contains(block1, block2, block3, block4) shouldBe false
    s.contains(block1, block2, block3, block5) shouldBe false
  }

  test("solves") {
    val s = SolvedQueryModel.empty.withField('a).withFields('b, 'c)
    val p = Pattern.empty[Expr].withEntity('a -> CTNode).withEntity('b -> CTNode).withEntity('c -> CTNode)

    s.solves(toField('a)) shouldBe true
    s.solves(toField('b)) shouldBe true
    s.solves(toField('x)) shouldBe false
    s.solves(p) shouldBe true
    s.solves(p.withEntity('x -> CTNode)) shouldBe false
  }

  test("solve relationship") {
    val s = SolvedQueryModel.empty

    an [IllegalArgumentException] should be thrownBy s.solveRelationship('a)
    s.solveRelationship('r -> CTRelationship) should equal(SolvedQueryModel.empty.withField('r -> CTRelationship))
    s.solveRelationship('r -> CTRelationship("KNOWS")) should equal(
      SolvedQueryModel.empty
        .withField('r -> CTRelationship)
        .withPredicate(HasType(Var("r")(CTRelationship("KNOWS")), RelType("KNOWS"))(CTBoolean))
    )
    s.solveRelationship('r -> CTRelationship("KNOWS", "LOVES", "HATES")) should equal(
      SolvedQueryModel.empty
        .withField('r -> CTRelationship)
        .withPredicate(Ors(
          HasType(Var("r")(), RelType("KNOWS"))(CTBoolean),
          HasType(Var("r")(), RelType("LOVES"))(CTBoolean),
          HasType(Var("r")(), RelType("HATES"))(CTBoolean)))
    )
  }
}
