package org.opencypher.spark.impl

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.util.SlotSymbolGenerator

class StdFrameSignatureTest extends StdTestSuite {

  implicit val planningContext = new PlanningContext(new SlotSymbolGenerator, null, null)

  test("adding a field") {
    val (field, sig) = StdFrameSignature.empty.addField('n -> CTNode)

    field should equal(StdField('n, CTNode))
    sig.field('n) should equal(field)
    sig.fields should equal(Seq(field))

    val slot = sig.slot(field)
    sig.slot(field.sym) should equal(slot)
    sig.slots should equal(Seq(slot))
  }

  test("adding several fields") {
    val (fields, sig) = StdFrameSignature.empty.addFields('n1 -> CTNode, 'n2 -> CTString, 'n3 -> CTBoolean)

    sig.fields should equal(fields)

    fields match {
      case Seq(n1, n2, n3) =>
        n1 should equal(StdField('n1, CTNode))
        n2 should equal(StdField('n2, CTString))
        n3 should equal(StdField('n3, CTBoolean))
        sig.field('n1) should equal(n1)
        sig.field('n2) should equal(n2)
        sig.field('n3) should equal(n3)
    }

    val slots = fields.map { field =>
      val slot = sig.slot(field)
      sig.slot(field.sym) should equal(slot)
      slot
    }
    sig.slots should equal(slots)
  }

  test("aliasing a field") {
    val (_, sig) = StdFrameSignature.empty.addField('n -> CTFloat)
    val (field, newSig) = sig.aliasField('n, 'newN)

    field should equal(StdField('newN, CTFloat))
    newSig.field('newN) should equal(field)
    an [IllegalArgumentException] should be thrownBy {
      newSig.field('n)
    }

    val slot = newSig.slot(field)
    newSig.slot(field.sym) should equal(slot)
    newSig.slots should equal(Seq(slot))
    an [IllegalArgumentException] should be thrownBy {
      newSig.slot('n)
    }
  }

  test("select fields") {
    val (_, sig) = StdFrameSignature.empty.addFields('n1 -> CTString, 'n2 -> CTRelationship, 'path -> CTPath)

    val (retainedOldSlotsSortedInNewOrder, newSig) = sig.selectFields('n2)

    retainedOldSlotsSortedInNewOrder.size shouldBe 1
    retainedOldSlotsSortedInNewOrder.head should equal(sig.slot('n2))
    newSig.fields.size shouldBe 1
    an [IllegalArgumentException] shouldBe thrownBy {
      newSig.field('n1)
    }
  }

}
