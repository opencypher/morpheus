package org.opencypher.spark.prototype.api.spark

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTNode}
import org.opencypher.spark.prototype.api.expr.{TrueLit, Var}
import org.opencypher.spark.prototype.api.record.{OpaqueField, ProjectedExpr, ProjectedField, RecordSlot}
import org.opencypher.spark.prototype.impl.syntax.spark.all._
import org.opencypher.spark.prototype.impl.util.{Added, Found, Replaced}

class SparkRecordsHeaderTest extends StdTestSuite {

  test("Can add projected expressions") {
    val content = ProjectedExpr(TrueLit, CTBoolean)
    val (result, Added(slot)) = SparkRecordsHeader.empty.update(addContent(content))

    slot should equal(RecordSlot(0, content))
    result.slots should equal(Seq(slot))
    result.fields should equal(Set.empty)
  }

  test("Can add opaque fields") {
    val content = OpaqueField(Var("n"), CTNode)
    val (result, Added(slot)) = SparkRecordsHeader.empty.update(addContent(content))

    slot should equal(RecordSlot(0, content))
    result.slots should equal(Seq(slot))
    result.fields should equal(Set(Var("n")))
  }

  test("Can re-add opaque fields") {
    val content = OpaqueField(Var("n"), CTNode)
    val (result, Found(slot)) = SparkRecordsHeader.empty.update(addContent(content).flatMap(_ => addContent(content)))

    slot should equal(RecordSlot(0, content))
    result.slots should equal(Seq(slot))
    result.fields should equal(Set(Var("n")))
  }

  test("Can add projected fields") {
    val content = ProjectedField(Var("n"), TrueLit, CTBoolean)
    val (result, Added(slot)) = SparkRecordsHeader.empty.update(addContent(content))

    slot should equal(RecordSlot(0, content))
    result.slots should equal(Seq(slot))
    result.fields should equal(Set(Var("n")))
  }

  test("Adding projected expressions re-uses previously added projected expressions") {
    val content = ProjectedExpr(TrueLit, CTBoolean)
    val (oldHeader, Added(oldSlot)) = SparkRecordsHeader.empty.update(addContent(content))
    val (newHeader, Found(newSlot)) = oldHeader.update(addContent(content))

    oldSlot should equal(RecordSlot(0, content))
    newSlot should equal(oldSlot)
    newHeader.slots should equal(Seq(newSlot))
    newHeader.fields should equal(Set.empty)
  }

  test("Adding projected expressions re-uses previously added projected fields") {
    val oldContent = ProjectedField(Var("n"), TrueLit, CTBoolean)
    val (oldHeader, Added(oldSlot)) = SparkRecordsHeader.empty.update(addContent(oldContent))
    val newContent = ProjectedExpr(TrueLit, CTBoolean)
    val (newHeader, Found(newSlot)) = oldHeader.update(addContent(newContent))

    oldSlot should equal(RecordSlot(0, oldContent))
    newSlot should equal(oldSlot)
    newHeader.slots should equal(Seq(newSlot))
    newHeader.fields should equal(Set(Var("n")))
  }

  test("Adding projected field will alias previously added projected expression") {
    val oldContent = ProjectedExpr(TrueLit, CTBoolean)
    val (oldHeader, Added(oldSlot)) = SparkRecordsHeader.empty.update(addContent(oldContent))
    val newContent = ProjectedField(Var("n"), TrueLit, CTBoolean)
    val (newHeader, Replaced(prevSlot, newSlot)) = oldHeader.update(addContent(newContent))

    oldSlot should equal(RecordSlot(0, oldContent))
    prevSlot should equal(oldSlot)
    newSlot should equal(RecordSlot(0, newContent))
    newHeader.slots should equal(Seq(newSlot))
    newHeader.fields should equal(Set(Var("n")))
  }
}
