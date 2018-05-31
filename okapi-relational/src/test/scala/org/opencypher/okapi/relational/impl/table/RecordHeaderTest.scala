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

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.ir.test._
import org.opencypher.okapi.testing.BaseTestSuite

import scala.language.implicitConversions

class RecordHeaderTest extends BaseTestSuite {

//  test("escape length 0 spark identifier") {
//    fromString("") should equal("_empty_")
//  }
//
//  test("escape length 1 spark identifiers") {
//    fromString("a") should equal("a")
//    fromString("1") should equal("_1")
//    fromString("_") should equal("_bar_")
//  }
//
//  test("escape length > 1 spark identifiers") {
//    fromString("aa") should equal("aa")
//    fromString("a1") should equal("a1")
//    fromString("_1") should equal("_bar_1")
//    fromString("a_") should equal("a_bar_")
//  }
//
//  test("escape weird chars") {
//    ".?!'\"`=@#$()^&%[]{}<>,:;|+*/\\-".foreach { ch =>
//      fromString(s"$ch").forall(esc => Character.isLetter(esc) || esc == '_')
//      fromString(s"a$ch").forall(esc => Character.isLetter(esc) || esc == '_')
//      fromString(s"1$ch").forall(esc => Character.isLetterOrDigit(esc) || esc == '_')
//      fromString(s"_$ch").forall(esc => Character.isLetter(esc) || esc == '_')
//    }
//  }
//
//  def fromString(text: String) = IRecordHeader.empty.from(text)

  test("select") {
    val nSlots = Seq(
      OpaqueField('n),
      ProjectedExpr(Property('n, PropertyKey("prop"))()),
      ProjectedExpr(HasLabel('n, Label("Foo"))())
    )
    val pSlots = Seq(
      OpaqueField('p),
      ProjectedExpr(Property('p, PropertyKey("prop"))())
    )

    val h1 = IRecordHeader.empty.addContents((nSlots ++ pSlots).toSeq)

    h1.select(Set('n)).contents should equal(nSlots)
    h1.select(Set('p)).contents should equal(pSlots)
    h1.select(Set('r)).contents should equal(Seq.empty)
    h1.select(Set('n, 'p, 'r)).contents should equal(nSlots ++ pSlots)
  }

  test("Can add projected expressions") {
    val content = ProjectedExpr(TrueLit())
    val result = IRecordHeader.empty.addContent(content)

    result.contents should equal(Seq(content))
    result.fields should equal(Set.empty)
  }

  test("Can add opaque fields") {
    val content = OpaqueField('n)
    val result = IRecordHeader.empty.addContent(content)

    result.contents should equal(Seq(content))
    result.fieldsAsVar should equal(Set(toVar('n)))
  }

  test("Can re-add opaque fields") {
    val content = OpaqueField('n)
    val result = IRecordHeader.empty.addContents(Seq(content, content))
    val slot = RecordSlot(0, content)

    result.contents should equal(Seq(content))
    result.fieldsAsVar should equal(Set(toVar('n)))
  }

  test("Can add projected fields") {
    val content = ProjectedField('n, TrueLit())
    val result = IRecordHeader.empty.addContent(content)

    result.contents should equal(Seq(content))
    result.fieldsAsVar should equal(Set(toVar('n)))
  }

  test("Adding projected expressions re-uses previously added projected expressions") {
    val content = ProjectedExpr(TrueLit())
    val oldHeader = IRecordHeader.empty.addContent(content)
    val newHeader = oldHeader.addContent(content)

    newHeader.contents should equal(Seq(content))
    newHeader.fieldsAsVar should equal(Set.empty)
  }

  // Requires Found
  ignore("Adding projected expressions re-uses previously added projected fields") {
    val oldContent = ProjectedField('n, TrueLit())
    val oldHeader = IRecordHeader.empty.addContent(oldContent)
    val newContent = ProjectedExpr(TrueLit())
    val newHeader = oldHeader.addContent(newContent)

    newHeader.contents should equal(Seq(newContent))
    newHeader.fieldsAsVar should equal(Set(toVar('n)))
  }

  test("Adding projected field will alias previously added projected expression") {
    val oldContent = ProjectedExpr(TrueLit())
    val oldHeader = IRecordHeader.empty.addContent(oldContent)
    val newContent = ProjectedField('n, TrueLit())
    val newHeader = oldHeader.addContent(newContent)

    newHeader.contents should equal(Seq(newContent))
    newHeader.fieldsAsVar should equal(Set(toVar('n)))
  }

  test("Adding projected field will alias previously added projected expression 2") {
    val oldContent = ProjectedExpr(Property('n, PropertyKey("prop"))())
    val oldHeader = IRecordHeader.empty.addContent(oldContent)
    val newContent = ProjectedField(Var("n.text")(CTString), Property('n, PropertyKey("prop"))())
    val newHeader = oldHeader.addContent(newContent)

    newHeader.contents should equal(Seq(newContent))
    newHeader.fieldsAsVar should equal(Set(Var("n.text")(CTString)))
  }

  test("Adding opaque field will replace previously existing") {
    val oldContent = OpaqueField(Var("n")(CTNode))
    val oldHeader = IRecordHeader.empty.addContent(oldContent)
    val newContent = OpaqueField(Var("n")(CTNode("User")))
    val newHeader = oldHeader.addContent(newContent)

    newHeader.contents should equal(Seq(newContent))
    newHeader.fieldsAsVar should equal(Set(Var("n")(CTNode("User"))))
  }

  test("concatenating headers") {
    var lhs = IRecordHeader.empty
    var rhs = IRecordHeader.empty

    lhs ++ rhs should equal(lhs)

    lhs = lhs.addContent(ProjectedExpr(Var("n")(CTNode)))
    lhs ++ rhs should equal(lhs)

    rhs = rhs.addContent(OpaqueField(Var("m")(CTRelationship)))
    (lhs ++ rhs).contents should equal(
      Seq(
        ProjectedExpr(Var("n")(CTNode)),
        OpaqueField(Var("m")(CTRelationship))
      ))
  }

  // TODO: do we support this behaviour? If yes, Property#equals needs to be overwritten again
  ignore("concatenating headers with similar properties") {
    val n = Var("n")()
    val lhs = IRecordHeader.empty.addContent(ProjectedExpr(Property(n, PropertyKey("name"))(CTInteger)))
    val rhs = IRecordHeader.empty.addContent(ProjectedExpr(Property(n, PropertyKey("name"))(CTString)))

    val concatenated = lhs ++ rhs

    concatenated.slots should equal(
      IndexedSeq(
        RecordSlot(0, ProjectedExpr(Property(n, PropertyKey("name"))(CTInteger))),
        RecordSlot(1, ProjectedExpr(Property(n, PropertyKey("name"))(CTString)))
      ))
  }

  test("can get labels") {
    val field1 = OpaqueField('n)
    val label1 = ProjectedExpr(HasLabel('n, Label("A"))(CTBoolean))
    val label2 = ProjectedExpr(HasLabel('n, Label("B"))(CTBoolean))
    val prop = ProjectedExpr(Property('n, PropertyKey("foo"))(CTString))
    val field2 = OpaqueField('m)
    val prop2 = ProjectedExpr(Property('m, PropertyKey("bar"))(CTString))

    val h1 = IRecordHeader.empty.addContent(field1)
    val h2 = h1.addContent(label1)
    val h3 = h2.addContent(label2)
    val h4 = h3.addContent(prop)
    val h5 = h4.addContent(field2)
    val header = h3.addContent(prop2)

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

    val h1 = IRecordHeader.empty.addContent(field1)
    val h2 = h1.addContent(label1)
    val h3 = h2.addContent(label2)
    val h4 = h3.addContent(prop)
    val h5 = h4.addContent(field2)
    val header = h5.addContent(prop2)

    header.childSlots(Var("n")(CTNode)) should equal(
      Seq(
        RecordSlot(1, label1),
        RecordSlot(2, label2),
        RecordSlot(3, prop)
      )
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

    val h1 = IRecordHeader.empty.addContent(field1)
    val h2 = h1.addContent(source1)
    val h3 = h2.addContent(target1)
    val h4 = h3.addContent(type1)
    val h5 = h4.addContent(prop1)
    val h6 = h5.addContent(field2)
    val h7 = h6.addContent(source2)
    val h8 = h7.addContent(target2)
    val h9 = h8.addContent(type2)
    val header = h9.addContent(prop2)

    header.childSlots(Var("e1")(CTRelationship)) should equal(
      Seq(
        RecordSlot(1, source1),
        RecordSlot(2, target1),
        RecordSlot(3, type1),
        RecordSlot(4, prop1)
      )
    )
  }

  test("labelSlots") {
    val field1 = OpaqueField('n)
    val field2 = OpaqueField('m)
    val label1 = ProjectedExpr(HasLabel('n, Label("A"))(CTBoolean))
    val label2 = ProjectedField('foo, HasLabel('n, Label("B"))(CTBoolean))
    val label3 = ProjectedExpr(HasLabel('m, Label("B"))(CTBoolean))
    val prop = ProjectedExpr(Property('n, PropertyKey("foo"))(CTString))

    val header = IRecordHeader.empty.addContents(Seq(field1, label1, label2, prop, field2, label3))

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

    val header = IRecordHeader.empty.addContents(
      Seq(
        node1,
        node2,
        rel,
        label,
        propFoo1,
        propFoo2,
        propBar1,
        propBar2,
        propBaz
      ))

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
    val header = IRecordHeader.empty.addContents(fields)

    header.nodesForType(CTNode) should equal(Seq(p, n, q))
    header.nodesForType(CTNode("Person")) should equal(Seq(p, n))
    header.nodesForType(CTNode("Fireman")) should equal(Seq(p, n))
    header.nodesForType(CTNode("Foo")) should equal(Seq(n, q))
    header.nodesForType(CTNode("Person", "Foo")) should equal(Seq(n))
    header.nodesForType(CTNode("Nop")) should equal(Seq.empty)
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
    val header = IRecordHeader.empty.addContents(fields)

    header.relationshipsForType(CTRelationship) should equal(List(p, r, q))
    header.relationshipsForType(CTRelationship("KNOWS")) should equal(List(p, r))
    header.relationshipsForType(CTRelationship("LOVES")) should equal(List(r, q))
    header.relationshipsForType(CTRelationship("LOVES", "HATES")) should equal(List(r, q))
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

    val nHeader = IRecordHeader.nodeFromSchema(n, schema)
    val aHeader = IRecordHeader.nodeFromSchema(a, schema)
    val bHeader = IRecordHeader.nodeFromSchema(b, schema)
    val cHeader = IRecordHeader.nodeFromSchema(c, schema)
    val abHeader = IRecordHeader.nodeFromSchema(ab, schema)

    nHeader should equal(
      IRecordHeader.empty
        .addContents(Seq(
          OpaqueField(n),
          ProjectedExpr(HasLabel(n, Label("A"))(CTBoolean)),
          ProjectedExpr(HasLabel(n, Label("B"))(CTBoolean)),
          ProjectedExpr(HasLabel(n, Label("C"))(CTBoolean)),
          ProjectedExpr(Property(n, PropertyKey("a"))(CTString.nullable)),
          ProjectedExpr(Property(n, PropertyKey("b"))(CTInteger.nullable)),
          ProjectedExpr(Property(n, PropertyKey("c"))(CTFloat.nullable)),
          ProjectedExpr(Property(n, PropertyKey("extra"))(CTBoolean.nullable)),
          ProjectedExpr(Property(n, PropertyKey("prop"))(CTString.nullable))
        )))

    aHeader should equal(
      IRecordHeader.empty
        .addContents(Seq(
          OpaqueField(a),
          ProjectedExpr(HasLabel(a, Label("A"))(CTBoolean)),
          ProjectedExpr(HasLabel(a, Label("B"))(CTBoolean)),
          ProjectedExpr(Property(a, PropertyKey("a"))(CTString.nullable)),
          ProjectedExpr(Property(a, PropertyKey("b"))(CTInteger.nullable)),
          ProjectedExpr(Property(a, PropertyKey("c"))(CTFloat.nullable))
        )))

    bHeader should equal(
      IRecordHeader.empty
        .addContents(Seq(
          OpaqueField(b),
          ProjectedExpr(HasLabel(b, Label("A"))(CTBoolean)),
          ProjectedExpr(HasLabel(b, Label("B"))(CTBoolean)),
          ProjectedExpr(Property(b, PropertyKey("a"))(CTString.nullable)),
          ProjectedExpr(Property(b, PropertyKey("b"))(CTInteger.nullable)),
          ProjectedExpr(Property(b, PropertyKey("c"))(CTFloat)),
          ProjectedExpr(Property(b, PropertyKey("extra"))(CTBoolean.nullable))
        )))

    cHeader should equal(
      IRecordHeader.empty
        .addContents(
          Seq(
            OpaqueField(c),
            ProjectedExpr(HasLabel(c, Label("C"))(CTBoolean))
          )))
        

    abHeader should equal(
      IRecordHeader.empty
        .addContents(Seq(
          OpaqueField(ab),
          ProjectedExpr(HasLabel(ab, Label("A"))(CTBoolean)),
          ProjectedExpr(HasLabel(ab, Label("B"))(CTBoolean)),
          ProjectedExpr(Property(ab, PropertyKey("a"))(CTString)),
          ProjectedExpr(Property(ab, PropertyKey("b"))(CTInteger.nullable)),
          ProjectedExpr(Property(ab, PropertyKey("c"))(CTFloat))
        )))
  }

  test("node from schema with implication") {
    val schema = Schema.empty
      .withNodePropertyKeys( Set.empty[String], Map("prop" -> CTString))
      .withNodePropertyKeys("A")("a" -> CTString.nullable)
      .withNodePropertyKeys("A", "X")("a" -> CTString.nullable, "x" -> CTFloat)
      .withNodePropertyKeys("B")("b" -> CTInteger, "extra" -> CTBoolean)
      .withNodePropertyKeys("A", "B", "X")("a" -> CTString, "b" -> CTInteger.nullable, "x" -> CTFloat)

    val x = Var("x")(CTNode("X"))

    val xHeader = IRecordHeader.nodeFromSchema(x, schema)

    xHeader should equal(
      IRecordHeader.empty
        .addContents(Seq(
          OpaqueField(x),
          ProjectedExpr(HasLabel(x, Label("A"))(CTBoolean)),
          ProjectedExpr(HasLabel(x, Label("B"))(CTBoolean)),
          ProjectedExpr(HasLabel(x, Label("X"))(CTBoolean)),
          ProjectedExpr(Property(x, PropertyKey("a"))(CTString.nullable)),
          ProjectedExpr(Property(x, PropertyKey("b"))(CTInteger.nullable)),
          ProjectedExpr(Property(x, PropertyKey("x"))(CTFloat))
        )))
  }

  test("relationship from schema") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("A")("a" -> CTString, "b" -> CTInteger.nullable)
      .withRelationshipPropertyKeys("B")("a" -> CTString, "c" -> CTFloat)

    val e = Var("e")(CTRelationship("A"))
    val r = Var("r")(CTRelationship)

    val eHeader = IRecordHeader.relationshipFromSchema(e, schema)
    val rHeader = IRecordHeader.relationshipFromSchema(r, schema)

    eHeader should equal(
      IRecordHeader.empty
        .addContents(Seq(
          ProjectedExpr(StartNode(e)(CTNode)),
          OpaqueField(e),
          ProjectedExpr(Type(e)(CTString)),
          ProjectedExpr(EndNode(e)(CTNode)),
          ProjectedExpr(Property(e, PropertyKey("a"))(CTString)),
          ProjectedExpr(Property(e, PropertyKey("b"))(CTInteger.nullable))
        )))

    rHeader should equal(
      IRecordHeader.empty
        .addContents(Seq(
          ProjectedExpr(StartNode(r)(CTNode)),
          OpaqueField(r),
          ProjectedExpr(Type(r)(CTString)),
          ProjectedExpr(EndNode(r)(CTNode)),
          ProjectedExpr(Property(r, PropertyKey("a"))(CTString)),
          ProjectedExpr(Property(r, PropertyKey("b"))(CTInteger.nullable)),
          ProjectedExpr(Property(r, PropertyKey("c"))(CTFloat.nullable))
        )))
  }

  test("relationship from schema with given relationship types") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("A")("a" -> CTString, "b" -> CTInteger.nullable)
      .withRelationshipPropertyKeys("B")("a" -> CTString, "b" -> CTInteger)

    val e = Var("e")(CTRelationship("A", "B"))

    val eHeader = IRecordHeader.relationshipFromSchema(e, schema)

    eHeader should equal(
      IRecordHeader.empty
        .addContents(Seq(
          ProjectedExpr(StartNode(e)(CTNode)),
          OpaqueField(e),
          ProjectedExpr(Type(e)(CTString)),
          ProjectedExpr(EndNode(e)(CTNode)),
          ProjectedExpr(Property(e, PropertyKey("a"))(CTString)),
          ProjectedExpr(Property(e, PropertyKey("b"))(CTInteger.nullable))
        )))
  }
}
