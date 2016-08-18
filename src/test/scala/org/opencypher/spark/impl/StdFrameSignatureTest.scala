package org.opencypher.spark.impl

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.util.SlotSymbolGenerator

class StdFrameSignatureTest extends StdTestSuite {

  implicit val planningContext = new PlanningContext(new SlotSymbolGenerator, null, null)

  test("adding a field") {
    val (field, sig) = StdFrameSignature.empty.addField('n -> CTNode)

    field should equal(StdField('n, CTNode))
    sig.field('n) should equal(Some(field))
    sig.fields should equal(Seq(field))

    val slot = sig.fieldSlot(field)
    sig.slot(field.sym) should equal(slot)
    sig.slots should equal(Seq(slot.get))
  }

  test("adding several fields") {
    val (fields, sig) = StdFrameSignature.empty.addFields('n1 -> CTNode, 'n2 -> CTString, 'n3 -> CTBoolean)

    sig.fields should equal(fields)

    fields match {
      case Seq(n1, n2, n3) =>
        n1 should equal(StdField('n1, CTNode))
        n2 should equal(StdField('n2, CTString))
        n3 should equal(StdField('n3, CTBoolean))
        sig.field('n1) should equal(Some(n1))
        sig.field('n2) should equal(Some(n2))
        sig.field('n3) should equal(Some(n3))
    }

    val slots = fields.flatMap { field =>
      val slot = sig.fieldSlot(field)
      sig.slot(field.sym) should equal(slot)
      slot
    }
    sig.slots should equal(slots)
  }

  test("aliasing a field") {
    val (_, sig) = StdFrameSignature.empty.addField('n -> CTFloat)
    val (field, newSig) = sig.aliasField('n, 'newN)

    field should equal(StdField('newN, CTFloat))
    newSig.field('newN) should equal(Some(field))
    newSig.field('n) should equal(None)

    val slot = newSig.fieldSlot(field)
    newSig.slot(field.sym) should equal(slot)
    newSig.slots should equal(Seq(slot.get))
    newSig.slot('n) should equal(None)
  }

  test("upcast a field") {
    val (_, sig) = StdFrameSignature.empty.addField('n -> CTFloat)
    val (field, newSig) = sig.upcastField('n, CTNumber)

    field should equal(StdField('n, CTNumber))
    newSig.field('n) should equal(Some(field))
    newSig.slot('n).get.cypherType should equal(CTNumber)
    an [IllegalArgumentException] should be thrownBy {
      newSig.upcastField('n, CTRelationship)
    }
  }

  test("select fields") {
    val (_, sig) = StdFrameSignature.empty.addFields('n1 -> CTString, 'n2 -> CTRelationship, 'path -> CTPath)

    val (retainedOldSlotsSortedInNewOrder, newSig) = sig.selectFields('n2)

    retainedOldSlotsSortedInNewOrder.size shouldBe 1
    retainedOldSlotsSortedInNewOrder.head should equal(sig.slot('n2).get)
    newSig.fields.size shouldBe 1
    newSig.field('n1) should equal(None)
  }
}
