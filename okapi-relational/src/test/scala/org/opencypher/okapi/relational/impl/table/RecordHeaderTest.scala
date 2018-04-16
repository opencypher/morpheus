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
package org.opencypher.okapi.relational.impl.table

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.ir.test._
import org.opencypher.okapi.relational.impl.table.RecordHeader._
import org.opencypher.okapi.test.BaseTestSuite

import scala.language.implicitConversions

class RecordHeaderTest extends BaseTestSuite {

  val n = Var("n")()
  val m = Var("m")()
  val p = Var("p")()
  val q = Var("q")()
  val r = Var("r")()
  val foo = Var("foo")()

  it("does not enforce order for equality checks") {
    val lhs = RecordHeader(n, p)
    val rhs = RecordHeader(p, n)

    lhs should equal(rhs)
  }

  test("select") {
    val nExprs: Set[Expr] = Set(
      n,
      Property(n, PropertyKey("prop"))(),
      HasLabel(n, Label("Foo"))()
    )
    val pExprs: Set[Expr] = Set(
      p,
      Property(p, PropertyKey("prop"))()
    )

    val header = RecordHeader.empty.withExpressions((nExprs ++ pExprs).toSeq: _*)

    header.selectField(n).expressions should equal(nExprs)
    header.selectField(p).expressions should equal(pExprs)
    header.selectField(r).expressions should equal(Set.empty)
    header.selectFields(Set[Var]('n, 'p, 'r)).expressions should equal(nExprs ++ pExprs)
  }

  test("Can add projected expressions") {
    val expr = TrueLit()
    val result = RecordHeader(expr)

    result.expressions should equal(Set(expr))
    result.fields should equal(Set.empty)
  }

  test("Can add opaque fields") {
    val expr = n
    val result = RecordHeader(expr)

    result.expressions should equal(Set(expr))
    result.fields should equal(Set(n))
  }

  test("Can re-add opaque fields") {
    val expr = n
    val result = RecordHeader(expr, expr)

    result.expressions should equal(Set(expr))
    result.fields should equal(Set(n))
  }

  test("Can add projected fields") {
    val result = RecordHeader.empty.withMapping(TrueLit() -> Set(n))

    result.mappings should equal(Set(TrueLit() -> Set(n)))
    result.fields should equal(Set(n))
  }

  test("Adding projected expressions re-uses previously added projected expressions") {
    val expr = TrueLit()
    val oldHeader = RecordHeader(expr)
    val newHeader = oldHeader.withExpression(expr)

    newHeader.expressions should equal(Set(expr))
    newHeader.fields should equal(Set.empty)
  }

  test("Adding projected expressions re-uses previously added projected fields") {
    val oldHeader = RecordHeader.empty.withMapping(TrueLit() -> Set(n))
    val newExpr = TrueLit()
    val newHeader = oldHeader.withExpression(newExpr)

    newHeader.expressions should equal(Set(newExpr))
    newHeader.fields should equal(Set(n))
  }

  test("Adding projected field will alias previously added projected expression") {
    val oldExpr = TrueLit()
    val oldHeader = RecordHeader(oldExpr)
    val newMapping = TrueLit() -> Set(n)
    val newHeader = oldHeader.withMapping(newMapping)

    newHeader.mappings should equal(Set(newMapping))
    newHeader.fields should equal(Set(n))
  }

  test("Adding projected field will alias previously added projected expression 2") {
    val oldExpr = Property(n, PropertyKey("prop"))()
    val oldHeader = RecordHeader(oldExpr)
    val newMapping = Property(n, PropertyKey("prop"))() -> Set(Var("n.text")(CTString))
    val newHeader = oldHeader.withMapping(newMapping)

    oldHeader.mappings should equal(Set(oldExpr -> Set.empty))
    newHeader.mappings should equal(Set(oldExpr -> Set(Var("n.text")(CTString))))
    oldHeader.expressions should be(newHeader.expressions)
    newHeader.fields should equal(Set(Var("n.text")(CTString)))
  }

  test("Adding opaque field will replace previously existing") {
    val oldHeader = RecordHeader(n)
    val newField = Var("n")(CTNode("User"))
    val newHeader = oldHeader.withField(newField)

    newHeader.expressions should equal(Set(newField))
    newHeader.fields.head.cypherType should equal(CTNode("User"))
  }

  test("concatenating headers") {
    var lhs = RecordHeader.empty
    var rhs = RecordHeader.empty

    lhs ++ rhs should equal(lhs)

    lhs = lhs.withExpression(Var("n")(CTNode))
    lhs ++ rhs should equal(lhs)

    rhs = rhs.withExpression(Var("m")(CTRelationship))
    (lhs ++ rhs).expressions should equal(Set(Var("n")(CTNode), Var("m")(CTRelationship)))
  }

  test("concatenating headers with similar properties") {
    val lhs = RecordHeader(Property(n, PropertyKey("name"))(CTInteger))
    val rhs = RecordHeader(Property(n, PropertyKey("name"))(CTString))

    val concatenated = lhs ++ rhs

    concatenated.expressions should equal(
      Set(
        Property(n, PropertyKey("name"))(CTInteger),
        Property(n, PropertyKey("name"))(CTString)
      ))
  }

  test("can get labels") {
    val label1 = HasLabel(n, Label("A"))(CTBoolean)
    val label2 = HasLabel(n, Label("B"))(CTBoolean)
    val prop = Property(n, PropertyKey("foo"))(CTString)

    val header = RecordHeader(n).withExpression(label1).withExpression(label2).withExpression(prop)

    header.labels(n) should equal(
      Seq(
        HasLabel(n, Label("A"))(CTBoolean),
        HasLabel(n, Label("B"))(CTBoolean)
      )
    )
  }

  test("can get all slots for a given node var") {
    val label1 = HasLabel(n, Label("A"))(CTBoolean)
    val label2 = HasLabel(n, Label("B"))(CTBoolean)
    val prop = Property(n, PropertyKey("foo"))(CTString)
    val prop2 = Property(m, PropertyKey("bar"))(CTString)

    val header = RecordHeader(n)
      .withExpression(label1)
      .withExpression(label2)
      .withExpression(prop)
      .withExpression(m)
      .withExpression(prop2)

    header.ownedExprs(Var("n")(CTNode)) should equal(
      RecordHeader(label1, label2, prop)
    )
  }

  test("can get all slots for a given edge var") {
    val field1 = Var("e1")()
    val source1 = StartNode(field1)(CTNode)
    val target1 = EndNode(field1)(CTNode)
    val type1 = HasType(field1, RelType("KNOWS"))(CTInteger)
    val prop1 = Property(field1, PropertyKey("foo"))(CTString)
    val field2 = Var("e2")()
    val source2 = StartNode(field2)(CTNode)
    val target2 = EndNode(field2)(CTNode)
    val type2 = HasType(field2, RelType("KNOWS"))(CTInteger)
    val prop2 = Property(field2, PropertyKey("bar"))(CTString)

    val header = RecordHeader(field1)
      .withExpression(source1)
      .withExpression(target1)
      .withExpression(type1)
      .withExpression(prop1)
      .withExpression(field2)
      .withExpression(source2)
      .withExpression(target2)
      .withExpression(type2)
      .withExpression(prop2)

    header.ownedExprs(Var("e1")(CTRelationship)) should equal(
      RecordHeader(source1, target1, type1, prop1)
    )
  }

  test("labelSlots") {
    val label1 = HasLabel(n, Label("A"))(CTBoolean) -> Set.empty[Var]
    val label2 = HasLabel(n, Label("B"))(CTBoolean) -> Set(foo)
    val label3 = HasLabel(m, Label("B"))(CTBoolean) -> Set.empty[Var]
    val prop = Property(n, PropertyKey("foo"))(CTString) -> Set.empty[Var]

    val header = RecordHeader.empty.withMappings(n -> Set(n), label1, label2, prop, m -> Set(m), label3)

    header.labelExprs(n).keySet should equal(Set(label1, label2))
    header.labelExprs(m).keySet should equal(Set(label3))
    header.labelExprs(q).keySet should equal(Set.empty)
  }

  test("propertySlots") {
    val label = HasLabel(n, Label("A"))(CTBoolean) -> Set.empty[Var]
    val propFoo1 = Property(n, PropertyKey("foo"))(CTString) -> Set.empty[Var]
    val propBar1 = Property(n, PropertyKey("bar"))(CTString) -> Set(foo)
    val propBaz = Property(n, PropertyKey("baz"))(CTString) -> Set.empty[Var]
    val propFoo2 = Property(r, PropertyKey("foo"))(CTString) -> Set.empty[Var]
    val propBar2 = Property(r, PropertyKey("bar"))(CTString) -> Set.empty[Var]

    val header = RecordHeader.empty
      .withMappings(n -> Set(n), m -> Set(m), r -> Set(r), label, propFoo1, propFoo2, propBar1, propBar2, propBaz)

    header.propertyExprs(n).keySet should equal(Set(propFoo1, propBar1, propBaz))
    header.propertyExprs(m).keySet should equal(Set.empty)
    header.propertyExprs(r).keySet should equal(Set(propFoo2, propBar2))
  }

  test("nodesForType") {
    val person: Var = 'p -> CTNode("Person")
    val fooLabelled: Var = 'q -> CTNode("Foo")
    val knows: Var = 'r -> CTRelationship("KNOWS")
    val header = RecordHeader.empty.withField(person).withField(n).withField(knows).withField(fooLabelled)
      .withExpressions(
        HasLabel(person, Label("Fireman"))(),
        HasLabel(n, Label("Person"))(),
        HasLabel(n, Label("Fireman"))(),
        HasLabel(n, Label("Foo"))()
      )

    header.nodesForType(CTNode) should equal(Set(person, n, fooLabelled))
    header.nodesForType(CTNode("Person")) should equal(Set(person, n))
    header.nodesForType(CTNode("Fireman")) should equal(Set(person, n))
    header.nodesForType(CTNode("Foo")) should equal(Set(n, fooLabelled))
    header.nodesForType(CTNode("Person", "Foo")) should equal(Set(n))
    header.nodesForType(CTNode("Nop")) should equal(Set.empty)
  }

  test("relsForType") {
    val p: Var = 'p -> CTRelationship("KNOWS")
    val r: Var = 'r -> CTRelationship
    val q: Var = 'q -> CTRelationship("LOVES", "HATES")
    val header = RecordHeader.empty.withFields(p, r, q, 'n -> CTNode("Foo"))

    header.relationshipsForType(CTRelationship) should equal(Set(p, r, q))
    header.relationshipsForType(CTRelationship("KNOWS")) should equal(Set(p, r))
    header.relationshipsForType(CTRelationship("LOVES")) should equal(Set(r, q))
    header.relationshipsForType(CTRelationship("LOVES", "HATES")) should equal(Set(r, q))
  }

  test("node from schema") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set.empty[String], Map("prop" -> CTString))
      .withNodePropertyKeys("A")("a" -> CTString.nullable)
      .withNodePropertyKeys("B")("b" -> CTInteger, "extra" -> CTBoolean, "c" -> CTFloat)
      .withNodePropertyKeys("A", "B")("a" -> CTString, "b" -> CTInteger.nullable, "c" -> CTFloat)
      .withNodePropertyKeys("C")()

    val a = Var("a")(CTNode("A"))
    val b = Var("b")(CTNode("B"))
    val c = Var("c")(CTNode("C"))
    val ab = Var("ab")(CTNode("A", "B"))

    RecordHeader.forNode(n, schema) should equal(
      RecordHeader.empty.withField(n).withExpressions(
        HasLabel(n, Label("A"))(CTBoolean),
        HasLabel(n, Label("B"))(CTBoolean),
        HasLabel(n, Label("C"))(CTBoolean),
        Property(n, PropertyKey("a"))(CTString.nullable),
        Property(n, PropertyKey("b"))(CTInteger.nullable),
        Property(n, PropertyKey("c"))(CTFloat.nullable),
        Property(n, PropertyKey("extra"))(CTBoolean.nullable),
        Property(n, PropertyKey("prop"))(CTString.nullable)
      )
    )

    RecordHeader.forNode(a, schema) should equal(
      RecordHeader.empty.withField(a).withExpressions(
        HasLabel(a, Label("A"))(CTBoolean),
        HasLabel(a, Label("B"))(CTBoolean),
        Property(a, PropertyKey("a"))(CTString.nullable),
        Property(a, PropertyKey("b"))(CTInteger.nullable),
        Property(a, PropertyKey("c"))(CTFloat.nullable)
      )
    )

    RecordHeader.forNode(b, schema) should equal(
      RecordHeader.empty.withField(b).withExpressions(
        HasLabel(b, Label("A"))(CTBoolean),
        HasLabel(b, Label("B"))(CTBoolean),
        Property(b, PropertyKey("a"))(CTString.nullable),
        Property(b, PropertyKey("b"))(CTInteger.nullable),
        Property(b, PropertyKey("c"))(CTFloat),
        Property(b, PropertyKey("extra"))(CTBoolean.nullable)
      )
    )

    RecordHeader.forNode(c, schema) should equal(
      RecordHeader.empty.withField(c).withExpression(HasLabel(c, Label("C"))(CTBoolean))
    )

    RecordHeader.forNode(ab, schema) should equal(
      RecordHeader.empty.withField(ab).withExpressions(
        HasLabel(ab, Label("A"))(CTBoolean),
        HasLabel(ab, Label("B"))(CTBoolean),
        Property(ab, PropertyKey("a"))(CTString),
        Property(ab, PropertyKey("b"))(CTInteger.nullable),
        Property(ab, PropertyKey("c"))(CTFloat)
      )
    )
  }

  test("node from schema with implication") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set.empty[String], Map("prop" -> CTString))
      .withNodePropertyKeys("A")("a" -> CTString.nullable)
      .withNodePropertyKeys("A", "X")("a" -> CTString.nullable, "x" -> CTFloat)
      .withNodePropertyKeys("B")("b" -> CTInteger, "extra" -> CTBoolean)
      .withNodePropertyKeys("A", "B", "X")("a" -> CTString, "b" -> CTInteger.nullable, "x" -> CTFloat)

    val x = Var("x")(CTNode("X"))

    RecordHeader.forNode(x, schema) should equal(
      RecordHeader.empty.withField(x).withExpressions(
        HasLabel(x, Label("A"))(CTBoolean),
        HasLabel(x, Label("B"))(CTBoolean),
        HasLabel(x, Label("X"))(CTBoolean),
        Property(x, PropertyKey("a"))(CTString.nullable),
        Property(x, PropertyKey("b"))(CTInteger.nullable),
        Property(x, PropertyKey("x"))(CTFloat)
      )
    )
  }

  test("relationship from schema") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("A")("a" -> CTString, "b" -> CTInteger.nullable)
      .withRelationshipPropertyKeys("B")("a" -> CTString, "c" -> CTFloat)

    val e = Var("e")(CTRelationship("A"))
    val r = Var("r")(CTRelationship)

    RecordHeader.forRelationship(e, schema) should equal(
      RecordHeader.empty.withField(e).withExpressions(
        StartNode(e)(CTNode),
        Type(e)(CTString),
        EndNode(e)(CTNode),
        Property(e, PropertyKey("a"))(CTString),
        Property(e, PropertyKey("b"))(CTInteger.nullable)
      )
    )

    RecordHeader.forRelationship(r, schema) should equal(
      RecordHeader.empty.withField(r).withExpressions(
        StartNode(r)(CTNode),
        Type(r)(CTString),
        EndNode(r)(CTNode),
        Property(r, PropertyKey("a"))(CTString),
        Property(r, PropertyKey("b"))(CTInteger.nullable),
        Property(r, PropertyKey("c"))(CTFloat.nullable)
      )
    )
  }

  test("relationship from schema with given relationship types") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("A")("a" -> CTString, "b" -> CTInteger.nullable)
      .withRelationshipPropertyKeys("B")("a" -> CTString, "b" -> CTInteger)

    val e = Var("e")(CTRelationship("A", "B"))

    val eHeader = RecordHeader.forRelationship(e, schema)

    eHeader should equal(
      RecordHeader.empty.withField(e).withExpressions(
        StartNode(e)(CTNode),
        Type(e)(CTString),
        EndNode(e)(CTNode),
        Property(e, PropertyKey("a"))(CTString),
        Property(e, PropertyKey("b"))(CTInteger.nullable)
      )
    )
  }
}
