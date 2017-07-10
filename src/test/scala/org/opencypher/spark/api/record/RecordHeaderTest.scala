package org.opencypher.spark.api.record

import org.opencypher.spark.BaseTestSuite
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{LabelRef, PropertyKey, PropertyKeyRef}
import org.opencypher.spark.impl.syntax.header._
import org.opencypher.spark.impl.util.{Added, Found, Removed, Replaced}
import org.opencypher.spark.toVar

import scala.language.implicitConversions

class RecordHeaderTest extends BaseTestSuite {

  test("Can add projected expressions") {
    val content = ProjectedExpr(TrueLit())
    val (result, Added(slot)) = RecordHeader.empty.update(addContent(content))

    slot should equal(RecordSlot(0, content))
    result.slots should equal(Seq(slot))
    result.fields should equal(Set.empty)
  }

  test("Can add opaque fields") {
    val content = OpaqueField('n)
    val (result, Added(slot)) = RecordHeader.empty.update(addContent(content))

    slot should equal(RecordSlot(0, content))
    result.slots should equal(Seq(slot))
    result.fields should equal(Set(toVar('n)))
  }

  test("Can re-add opaque fields") {
    val content = OpaqueField('n)
    val (result, slots) = RecordHeader.empty.update(addContents(Seq(content, content)))
    val slot = RecordSlot(0, content)

    slots should equal(Vector(Added(slot), Found(slot)))
    result.slots should equal(Seq(slot))
    result.fields should equal(Set(toVar('n)))
  }

  test("Can add projected fields") {
    val content = ProjectedField('n, TrueLit())
    val (result, Added(slot)) = RecordHeader.empty.update(addContent(content))

    slot should equal(RecordSlot(0, content))
    result.slots should equal(Seq(slot))
    result.fields should equal(Set(toVar('n)))
  }

  test("Adding projected expressions re-uses previously added projected expressions") {
    val content = ProjectedExpr(TrueLit())
    val (oldHeader, Added(oldSlot)) = RecordHeader.empty.update(addContent(content))
    val (newHeader, Found(newSlot)) = oldHeader.update(addContent(content))

    oldSlot should equal(RecordSlot(0, content))
    newSlot should equal(oldSlot)
    newHeader.slots should equal(Seq(newSlot))
    newHeader.fields should equal(Set.empty)
  }

  test("Adding projected expressions re-uses previously added projected fields") {
    val oldContent = ProjectedField('n, TrueLit())
    val (oldHeader, Added(oldSlot)) = RecordHeader.empty.update(addContent(oldContent))
    val newContent = ProjectedExpr(TrueLit())
    val (newHeader, Found(newSlot)) = oldHeader.update(addContent(newContent))

    oldSlot should equal(RecordSlot(0, oldContent))
    newSlot should equal(oldSlot)
    newHeader.slots should equal(Seq(newSlot))
    newHeader.fields should equal(Set(toVar('n)))
  }

  test("Adding projected field will alias previously added projected expression") {
    val oldContent = ProjectedExpr(TrueLit())
    val (oldHeader, Added(oldSlot)) = RecordHeader.empty.update(addContent(oldContent))
    val newContent = ProjectedField('n, TrueLit())
    val (newHeader, Replaced(prevSlot, newSlot)) = oldHeader.update(addContent(newContent))

    oldSlot should equal(RecordSlot(0, oldContent))
    prevSlot should equal(oldSlot)
    newSlot should equal(RecordSlot(0, newContent))
    newHeader.slots should equal(Seq(newSlot))
    newHeader.fields should equal(Set(toVar('n)))
  }

  test("Adding projected field will alias previously added projected expression 2") {
    val oldContent = ProjectedExpr(Property('n, PropertyKey("prop"))())
    val (oldHeader, Added(oldSlot)) = RecordHeader.empty.update(addContent(oldContent))
    val newContent = ProjectedField(Var("n.text")(CTString), Property('n, PropertyKey("prop"))())
    val (newHeader, Replaced(prevSlot, newSlot)) = oldHeader.update(addContent(newContent))

    oldSlot should equal(RecordSlot(0, oldContent))
    prevSlot should equal(oldSlot)
    newSlot should equal(RecordSlot(0, newContent))
    newHeader.slots should equal(Seq(newSlot))
    newHeader.fields should equal(Set(Var("n.text")(CTString)))
  }

  test("Adding opaque field will replace previously existing") {
    val oldContent = OpaqueField(Var("n")(CTNode))
    val (oldHeader, Added(oldSlot)) = RecordHeader.empty.update(addContent(oldContent))
    val newContent = OpaqueField(Var("n")(CTNode("User")))
    val (newHeader, Replaced(prevSlot, newSlot)) = oldHeader.update(addContent(newContent))

    oldSlot should equal(RecordSlot(0, oldContent))
    prevSlot should equal(oldSlot)
    newSlot should equal(RecordSlot(0, newContent))
    newHeader.slots should equal(Seq(newSlot))
    newHeader.fields should equal(Set(Var("n")(CTNode("User"))))
  }

//  test("selecting an independent slot") {
//    val (h1, Added(nSlot)) = RecordHeader.empty.update(addContent(OpaqueField('n)))
//    val (h2, Added(mSlot)) = h1.update(addContent(OpaqueField('m)))
//    val (h3, Added(oSlot)) = h2.update(addContent(OpaqueField('o)))
//    val (h4, removed) = h3.update(selectFields(s => s.index == nSlot.index))
//
//    removed should equal(Vector(Removed(mSlot, Set.empty), Removed(oSlot, Set.empty)))
//    h4.slots should equal(Seq(nSlot))
//  }
//
//  test("selecting two independent slots") {
//    val (h1, Added(nSlot)) = RecordHeader.empty.update(addContent(OpaqueField('n)))
//    val (h2, Added(mSlot)) = h1.update(addContent(OpaqueField('m)))
//    val (h3, Added(oSlot)) = h2.update(addContent(OpaqueField('o)))
//    val (h4, removed) = h3.update(selectFields(s => s.index < 2))
//
//    removed should equal(Vector(Removed(oSlot, Set.empty)))
//    h4.slots should equal(Seq(nSlot, mSlot))
//  }

  // TODO: is this what we want? Probably not
  ignore("Adding opaque field will replace previously existing and update dependent expressions") {
//    val oldField = OpaqueField(Var("n", CTNode))
//    val oldExpr = ProjectedExpr(HasLabel(Var("n", CTNode), LabelRef(0), CTBoolean))
//    val (oldHeader, _) = RecordHeader.empty.update(addContents(Seq(oldField, oldExpr)))
//    val newContent = OpaqueField(Var("n", CTNode("User")))
//    val (newHeader, Replaced(_, newSlot)) = oldHeader.update(addContent(newContent))
//    val newExprSlot = RecordSlot(1, ProjectedExpr(HasLabel(Var("n", CTNode("User")), LabelRef(0), CTBoolean)))
//
//    newSlot should equal(RecordSlot(0, newContent))
//    newHeader.slots should equal(Seq(newSlot, newExprSlot))
//    newHeader.fields should equal(Set(Var("n", CTNode("User"))))
  }

  test("concatenating headers") {
    var lhs = RecordHeader.empty
    var rhs = RecordHeader.empty

    lhs ++ rhs should equal(lhs)

    lhs = lhs.update(addContent(ProjectedExpr(Var("n")(CTNode))))._1
    lhs ++ rhs should equal(lhs)

    rhs = rhs.update(addContent(OpaqueField(Var("m")(CTRelationship))))._1
    (lhs ++ rhs).slots.map(_.content) should equal(Seq(
      ProjectedExpr(Var("n")(CTNode)), OpaqueField(Var("m")(CTRelationship))
    ))
  }

  test("concatenating headers with similar properties") {
    val (lhs, _) = RecordHeader.empty.update(addContent(ProjectedExpr(Property(Var("n")(), PropertyKey("name"))(CTInteger))))
    val (rhs, _) = RecordHeader.empty.update(addContent(ProjectedExpr(Property(Var("n")(), PropertyKey("name"))(CTString))))

    val concatenated = lhs ++ rhs

    concatenated.slots should equal(IndexedSeq(
      RecordSlot(0, ProjectedExpr(Property(Var("n")(), PropertyKey("name"))(CTInteger))),
      RecordSlot(1, ProjectedExpr(Property(Var("n")(), PropertyKey("name"))(CTString)))
    ))
  }
}
