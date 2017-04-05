package org.opencypher.spark_legacy.impl

import org.opencypher.spark.api.types._
import org.opencypher.spark_legacy.impl.util.SlotSymbolGenerator
import org.opencypher.spark.StdTestSuite

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

  test("fails when adding a field twice (1)") {
    val (_, s1) = StdFrameSignature.empty.addField('n -> CTNode)
    val error = the [StdFrameSignature.FieldAlreadyExists] thrownBy {
      s1.addField('n -> CTInteger)
    }
    error.contextName should equal("requireNoSuchFieldExists")
  }

  test("fails when adding a field twice (2)") {
    val error = the [StdFrameSignature.FieldAlreadyExists] thrownBy {
      StdFrameSignature.empty.addFields('n -> CTNode, 'n -> CTInteger)
    }
    error.contextName should equal("requireNoSuchFieldExists")
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
    val (field, newSig) = sig.aliasField('n -> 'newN)

    field should equal(StdField('newN, CTFloat))
    newSig.field('newN) should equal(Some(field))
    newSig.field('n) should equal(None)

    val slot = newSig.fieldSlot(field)
    newSig.slot(field.sym) should equal(slot)
    newSig.slots should equal(Seq(slot.get))
    newSig.slot('n) should equal(None)
  }

  test("aliasing a field to an already existing field") {
    val (_, sig) = StdFrameSignature.empty.addFields('n -> CTFloat, 'm -> CTFloat)

    val error = the [StdFrameSignature.FieldAlreadyExists] thrownBy {
      sig.aliasField('n -> 'm)
    }
    error.contextName should equal("requireNoSuchFieldExists")
  }

  test("upcast a field") {
    val (_, sig) = StdFrameSignature.empty.addField('n -> CTFloat)
    val (field, newSig) = sig.upcastField('n -> CTNumber)

    field should equal(StdField('n, CTNumber))
    newSig.field('n) should equal(Some(field))
    newSig.slot('n).get.cypherType should equal(CTNumber)
    an [IllegalArgumentException] should be thrownBy {
      newSig.upcastField('n -> CTRelationship)
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

  test("concatenating signatures") {
    val (_, sig1) = StdFrameSignature.empty.addFields('n4 -> CTAny, 'n1 -> CTAny)
    val (_, sig2) = StdFrameSignature.empty.addFields('n3 -> CTAny, 'n2 -> CTAny)

    val concatenated = sig1 ++ sig2

    val fieldToOrdinal = concatenated.fields.map { f =>
      f.sym -> concatenated.slot(f.sym).get.ordinal
    }.sortBy(_._2)

    fieldToOrdinal should equal(Seq('n4 -> 0, 'n1 -> 1, 'n3 -> 2, 'n2 -> 3))
  }

  test("fields should be returned in slot order") {
    val (_, sig1) = StdFrameSignature.empty.addField('n1 -> CTAny)._2.addFields('n2 -> CTAny, 'n3 -> CTAny)
    val (_, sig2) = StdFrameSignature.empty.addFields('n4 -> CTAny, 'n5 -> CTAny, 'n6 -> CTAny)
    val (_, sig) = (sig2 ++ sig1).selectFields('n2, 'n4, 'n6)

    val sortedFields = sig.fields.sortBy { f =>
      sig.slot(f.sym).get.ordinal
    }

    sig.fields should equal(sortedFields)
  }

  test("dropping a field") {
    val (_, sig) = StdFrameSignature.empty.addFields('n1 -> CTAny, 'n2 -> CTInteger, 'n3 -> CTBoolean)
    val dropped = sig.dropField('n2)

    dropped.fieldOrdinals should equal(Seq('n1 -> 0, 'n3 -> 1))

    val dropped2 = dropped.addField('n4 -> CTNode)._2.dropField('n1)

    dropped2.fieldOrdinals should equal(Seq('n3 -> 0, 'n4 -> 1))
  }
}
