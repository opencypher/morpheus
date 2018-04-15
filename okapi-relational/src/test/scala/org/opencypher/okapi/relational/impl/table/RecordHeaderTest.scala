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

  it("does not enforce order for equality checks") {
    val lhs = RecordHeader(OpaqueField(Var("a")()), OpaqueField(Var("b")()))
    val rhs = RecordHeader(OpaqueField(Var("b")()), OpaqueField(Var("a")()))

    lhs should equal(rhs)
  }

  test("select") {
    val nSlots: Set[SlotContent] = Set(
      OpaqueField('n),
      ProjectedExpr(Property('n, PropertyKey("prop"))()),
      ProjectedExpr(HasLabel('n, Label("Foo"))())
    )
    val pSlots: Set[SlotContent] = Set(
      OpaqueField('p),
      ProjectedExpr(Property('p, PropertyKey("prop"))())
    )

    val h1 = RecordHeader.fromSlotContents(nSlots ++ pSlots)

    h1.select('n).contents should equal(nSlots)
    h1.select('p).contents should equal(pSlots)
    h1.select('r).contents should equal(Set.empty)
    h1.select(Set[Var]('n, 'p, 'r)).contents should equal(nSlots ++ pSlots)
  }

  test("Can add projected expressions") {
    val content = ProjectedExpr(TrueLit())
    val result = RecordHeader(content)

    result.slots should equal(Set(content.toRecordSlot))
    result.fields should equal(Set.empty)
  }

  test("Can add opaque fields") {
    val content = OpaqueField('n)
    val result = RecordHeader(content)

    result.slots should equal(Set(content.toRecordSlot))
    result.fields should equal(Set(toVar('n)))
  }

  test("Can re-add opaque fields") {
    val content = OpaqueField('n)
    val result = RecordHeader(content, content)

    result.slots should equal(Set(content.toRecordSlot))
    result.fields should equal(Set(toVar('n)))
  }

  test("Can add projected fields") {
    val content = ProjectedField('n, TrueLit())
    val result = RecordHeader(content)

    result.slots should equal(Set(content.toRecordSlot))
    result.fields should equal(Set(toVar('n)))
  }

  test("Adding projected expressions re-uses previously added projected expressions") {
    val content = ProjectedExpr(TrueLit())
    val oldHeader = RecordHeader(content)
    val newHeader = oldHeader.withSlotContents(content)

    newHeader.slots should equal(Set(content.toRecordSlot))
    newHeader.fields should equal(Set.empty)
  }

  //TODO: Do this in operator and add test for operator instead
  //  test("Adding projected expressions re-uses previously added projected fields") {
  //    val oldContent = ProjectedField('n, TrueLit())
  //    val oldHeader = RecordHeader(oldContent)
  //    val newContent = ProjectedExpr(TrueLit())
  //    val newHeader = oldHeader.withSlotContent(newContent)
  //
  //    newHeader.slots should equal(Set(newContent.toRecordSlot))
  //    newHeader.fields should equal(Set(toVar('n).name))
  //  }
  //
  //  test("Adding projected field will alias previously added projected expression") {
  //    val oldContent = ProjectedExpr(TrueLit())
  //    val oldHeader = RecordHeader(oldContent)
  //    val newContent = ProjectedField('n, TrueLit())
  //    val newHeader = oldHeader.withSlotContent(newContent)
  //
  //    newHeader.slots should equal(Set(newContent.))
  //    newHeader.internalHeader.fields should equal(Set(toVar('n)))
  //  }
  //
  //  test("Adding projected field will alias previously added projected expression 2") {
  //    val oldContent = ProjectedExpr(Property('n, PropertyKey("prop"))())
  //    val (oldHeader, Added(oldSlot)) = RecordHeader.empty.update(addContent(oldContent))
  //    val newContent = ProjectedField(Var("n.text")(CTString), Property('n, PropertyKey("prop"))())
  //    val (newHeader, Replaced(prevSlot, newSlot)) = oldHeader.update(addContent(newContent))
  //
  //    oldSlot should equal(RecordSlot(0, oldContent))
  //    prevSlot should equal(oldSlot)
  //    newSlot should equal(RecordSlot(0, newContent))
  //    newHeader.slots should equal(Seq(newSlot))
  //    newHeader.internalHeader.fields should equal(Set(Var("n.text")(CTString)))
  //  }

  test("Adding opaque field will replace previously existing") {
    val oldContent = OpaqueField(Var("n")(CTNode))
    val oldHeader = RecordHeader(oldContent)
    val newContent = OpaqueField(Var("n")(CTNode("User")))
    val newHeader = oldHeader.withSlotContent(newContent)

    newHeader.slots should equal(Set(newContent.toRecordSlot))
    newHeader.fields should equal(Set(Var("n")(CTNode("User"))))
  }

  test("concatenating headers") {
    var lhs = RecordHeader.empty
    var rhs = RecordHeader.empty

    lhs ++ rhs should equal(lhs)

    lhs = lhs.withSlotContent(ProjectedExpr(Var("n")(CTNode)))
    lhs ++ rhs should equal(lhs)

    rhs = rhs.withSlotContent(OpaqueField(Var("m")(CTRelationship)))
    (lhs ++ rhs).contents should equal(
      Set(
        ProjectedExpr(Var("n")(CTNode)),
        OpaqueField(Var("m")(CTRelationship))
      ))
  }

  test("concatenating headers with similar properties") {
    val n = Var("n")()
    val lhs = RecordHeader(ProjectedExpr(Property(n, PropertyKey("name"))(CTInteger)))
    val rhs = RecordHeader(ProjectedExpr(Property(n, PropertyKey("name"))(CTString)))

    val concatenated = lhs ++ rhs

    concatenated.contents should equal(
      Set(
        ProjectedExpr(Property(n, PropertyKey("name"))(CTInteger)),
        ProjectedExpr(Property(n, PropertyKey("name"))(CTString))
      ))
  }

  test("can get labels") {
    val field1 = OpaqueField('n)
    val label1 = ProjectedExpr(HasLabel('n, Label("A"))(CTBoolean))
    val label2 = ProjectedExpr(HasLabel('n, Label("B"))(CTBoolean))
    val prop = ProjectedExpr(Property('n, PropertyKey("foo"))(CTString))

    val header = RecordHeader(field1).withSlotContent(label1).withSlotContent(label2).withSlotContent(prop)

    header.labels(Var("n")(CTNode)) should equal(
      Seq(
        HasLabel('n, Label("A"))(CTBoolean),
        HasLabel('n, Label("B"))(CTBoolean)
      )
    )
  }

  test("can get all slots for a given node var") {
    val field1 = OpaqueField('n)
    val label1 = ProjectedExpr(HasLabel('n, Label("A"))(CTBoolean))
    val label2 = ProjectedExpr(HasLabel('n, Label("B"))(CTBoolean))
    val prop = ProjectedExpr(Property('n, PropertyKey("foo"))(CTString))
    val field2 = OpaqueField('m)
    val prop2 = ProjectedExpr(Property('m, PropertyKey("bar"))(CTString))

    val header = RecordHeader(field1)
      .withSlotContent(label1)
      .withSlotContent(label2)
      .withSlotContent(prop)
      .withSlotContent(field2)
      .withSlotContent(prop2)

    header.childSlots(Var("n")(CTNode)) should equal(
      RecordHeader(label1, label2, prop)
    )
  }

  test("can get all slots for a given edge var") {
    val field1 = OpaqueField('e1)
    val source1 = ProjectedExpr(StartNode('e1)(CTNode))
    val target1 = ProjectedExpr(EndNode('e1)(CTNode))
    val type1 = ProjectedExpr(HasType('e1, RelType("KNOWS"))(CTInteger))
    val prop1 = ProjectedExpr(Property('e1, PropertyKey("foo"))(CTString))
    val field2 = OpaqueField('e2)
    val source2 = ProjectedExpr(StartNode('e2)(CTNode))
    val target2 = ProjectedExpr(EndNode('e2)(CTNode))
    val type2 = ProjectedExpr(HasType('e2, RelType("KNOWS"))(CTInteger))
    val prop2 = ProjectedExpr(Property('e2, PropertyKey("bar"))(CTString))

    val header = RecordHeader(field1)
      .withSlotContent(source1)
      .withSlotContent(target1)
      .withSlotContent(type1)
      .withSlotContent(prop1)
      .withSlotContent(field2)
      .withSlotContent(source2)
      .withSlotContent(target2)
      .withSlotContent(type2)
      .withSlotContent(prop2)

    header.childSlots(Var("e1")(CTRelationship)) should equal(
      RecordHeader(source1, target1, type1, prop1)
    )
  }

  test("labelSlots") {
    val field1 = OpaqueField('n)
    val field2 = OpaqueField('m)
    val label1 = ProjectedExpr(HasLabel('n, Label("A"))(CTBoolean))
    val label2 = ProjectedField('foo, HasLabel('n, Label("B"))(CTBoolean))
    val label3 = ProjectedExpr(HasLabel('m, Label("B"))(CTBoolean))
    val prop = ProjectedExpr(Property('n, PropertyKey("foo"))(CTString))

    val header = RecordHeader(field1, label1, label2, prop, field2, label3)

    header.labelSlots('n).mapValues(_.content) should equal(Map(label1.expr -> label1, label2.expr -> label2))
    header.labelSlots('m).mapValues(_.content) should equal(Map(label3.expr -> label3))
    header.labelSlots('q).mapValues(_.content) should equal(Map.empty)
  }

  test("propertySlots") {
    val node1 = OpaqueField('n)
    val node2 = OpaqueField('m)
    val rel = OpaqueField('r)
    val label = ProjectedExpr(HasLabel('n, Label("A"))(CTBoolean))
    val propFoo1 = ProjectedExpr(Property('n, PropertyKey("foo"))(CTString))
    val propBar1 = ProjectedField('foo, Property('n, PropertyKey("bar"))(CTString))
    val propBaz = ProjectedExpr(Property('n, PropertyKey("baz"))(CTString))
    val propFoo2 = ProjectedExpr(Property('r, PropertyKey("foo"))(CTString))
    val propBar2 = ProjectedExpr(Property('r, PropertyKey("bar"))(CTString))

    val header = RecordHeader(node1, node2, rel, label, propFoo1, propFoo2, propBar1, propBar2, propBaz)

    header.propertySlots('n).mapValues(_.content) should equal(
      Map(propFoo1.expr -> propFoo1, propBar1.expr -> propBar1, propBaz.expr -> propBaz))
    header.propertySlots('m).mapValues(_.content) should equal(Map.empty)
    header.propertySlots('r).mapValues(_.content) should equal(
      Map(propFoo2.expr -> propFoo2, propBar2.expr -> propBar2))
  }

  test("nodesForType") {
    val p: Var = 'p -> CTNode("Person")
    val n: Var = 'n -> CTNode
    val q: Var = 'q -> CTNode("Foo")
    val fields = Seq(
      OpaqueField(p),
      ProjectedExpr(HasLabel(p, Label("Fireman"))()),
      OpaqueField(n),
      ProjectedExpr(HasLabel(n, Label("Person"))()),
      ProjectedExpr(HasLabel(n, Label("Fireman"))()),
      ProjectedExpr(HasLabel(n, Label("Foo"))()),
      OpaqueField(q),
      OpaqueField('r -> CTRelationship("KNOWS"))
    )
    val header = RecordHeader(fields: _*)

    header.nodesForType(CTNode) should equal(Set(p, n, q))
    header.nodesForType(CTNode("Person")) should equal(Set(p, n))
    header.nodesForType(CTNode("Fireman")) should equal(Set(p, n))
    header.nodesForType(CTNode("Foo")) should equal(Set(n, q))
    header.nodesForType(CTNode("Person", "Foo")) should equal(Set(n))
    header.nodesForType(CTNode("Nop")) should equal(Set.empty)
  }

  test("relsForType") {
    val p: Var = 'p -> CTRelationship("KNOWS")
    val r: Var = 'r -> CTRelationship
    val q: Var = 'q -> CTRelationship("LOVES", "HATES")
    val fields = Seq(
      OpaqueField(p),
      OpaqueField(r),
      OpaqueField(q),
      OpaqueField('n -> CTNode("Foo"))
    )
    val header = RecordHeader(fields: _*)

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

    val n = Var("n")(CTNode)
    val a = Var("a")(CTNode("A"))
    val b = Var("b")(CTNode("B"))
    val c = Var("c")(CTNode("C"))
    val ab = Var("ab")(CTNode("A", "B"))

    RecordHeader.forNode(n, schema) should equal(
      RecordHeader(
        OpaqueField(n),
        ProjectedExpr(HasLabel(n, Label("A"))(CTBoolean)),
        ProjectedExpr(HasLabel(n, Label("B"))(CTBoolean)),
        ProjectedExpr(HasLabel(n, Label("C"))(CTBoolean)),
        ProjectedExpr(Property(n, PropertyKey("a"))(CTString.nullable)),
        ProjectedExpr(Property(n, PropertyKey("b"))(CTInteger.nullable)),
        ProjectedExpr(Property(n, PropertyKey("c"))(CTFloat.nullable)),
        ProjectedExpr(Property(n, PropertyKey("extra"))(CTBoolean.nullable)),
        ProjectedExpr(Property(n, PropertyKey("prop"))(CTString.nullable))
      )
    )

    RecordHeader.forNode(a, schema) should equal(
      RecordHeader(
        OpaqueField(a),
        ProjectedExpr(HasLabel(a, Label("A"))(CTBoolean)),
        ProjectedExpr(HasLabel(a, Label("B"))(CTBoolean)),
        ProjectedExpr(Property(a, PropertyKey("a"))(CTString.nullable)),
        ProjectedExpr(Property(a, PropertyKey("b"))(CTInteger.nullable)),
        ProjectedExpr(Property(a, PropertyKey("c"))(CTFloat.nullable))
      )
    )

    RecordHeader.forNode(b, schema) should equal(
      RecordHeader(
        OpaqueField(b),
        ProjectedExpr(HasLabel(b, Label("A"))(CTBoolean)),
        ProjectedExpr(HasLabel(b, Label("B"))(CTBoolean)),
        ProjectedExpr(Property(b, PropertyKey("a"))(CTString.nullable)),
        ProjectedExpr(Property(b, PropertyKey("b"))(CTInteger.nullable)),
        ProjectedExpr(Property(b, PropertyKey("c"))(CTFloat)),
        ProjectedExpr(Property(b, PropertyKey("extra"))(CTBoolean.nullable))
      )
    )

    RecordHeader.forNode(c, schema) should equal(
      RecordHeader(
        OpaqueField(c),
        ProjectedExpr(HasLabel(c, Label("C"))(CTBoolean))
      )
    )

    RecordHeader.forNode(ab, schema) should equal(
      RecordHeader(
        OpaqueField(ab),
        ProjectedExpr(HasLabel(ab, Label("A"))(CTBoolean)),
        ProjectedExpr(HasLabel(ab, Label("B"))(CTBoolean)),
        ProjectedExpr(Property(ab, PropertyKey("a"))(CTString)),
        ProjectedExpr(Property(ab, PropertyKey("b"))(CTInteger.nullable)),
        ProjectedExpr(Property(ab, PropertyKey("c"))(CTFloat))
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
      RecordHeader(
        OpaqueField(x),
        ProjectedExpr(HasLabel(x, Label("A"))(CTBoolean)),
        ProjectedExpr(HasLabel(x, Label("B"))(CTBoolean)),
        ProjectedExpr(HasLabel(x, Label("X"))(CTBoolean)),
        ProjectedExpr(Property(x, PropertyKey("a"))(CTString.nullable)),
        ProjectedExpr(Property(x, PropertyKey("b"))(CTInteger.nullable)),
        ProjectedExpr(Property(x, PropertyKey("x"))(CTFloat))
      )
    )
  }

  test("relationship from schema") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("A")("a" -> CTString, "b" -> CTInteger.nullable)
      .withRelationshipPropertyKeys("B")("a" -> CTString, "c" -> CTFloat)

    val e = Var("e")(CTRelationship("A"))
    val r = Var("r")(CTRelationship)

    val rHeader = RecordHeader.forRelationship(r, schema)

    RecordHeader.forRelationship(e, schema) should equal(
      RecordHeader(
        ProjectedExpr(StartNode(e)(CTNode)),
        OpaqueField(e),
        ProjectedExpr(Type(e)(CTString)),
        ProjectedExpr(EndNode(e)(CTNode)),
        ProjectedExpr(Property(e, PropertyKey("a"))(CTString)),
        ProjectedExpr(Property(e, PropertyKey("b"))(CTInteger.nullable))
      )
    )

    rHeader should equal(
      RecordHeader(
        ProjectedExpr(StartNode(r)(CTNode)),
        OpaqueField(r),
        ProjectedExpr(Type(r)(CTString)),
        ProjectedExpr(EndNode(r)(CTNode)),
        ProjectedExpr(Property(r, PropertyKey("a"))(CTString)),
        ProjectedExpr(Property(r, PropertyKey("b"))(CTInteger.nullable)),
        ProjectedExpr(Property(r, PropertyKey("c"))(CTFloat.nullable))
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
      RecordHeader(
        ProjectedExpr(StartNode(e)(CTNode)),
        OpaqueField(e),
        ProjectedExpr(Type(e)(CTString)),
        ProjectedExpr(EndNode(e)(CTNode)),
        ProjectedExpr(Property(e, PropertyKey("a"))(CTString)),
        ProjectedExpr(Property(e, PropertyKey("b"))(CTInteger.nullable))
      )
    )
  }
}
