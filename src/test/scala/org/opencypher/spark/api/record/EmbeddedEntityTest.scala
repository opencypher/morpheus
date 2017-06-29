package org.opencypher.spark.api.record

import org.opencypher.spark.BaseTestSuite
import org.opencypher.spark.api.exception.SparkCypherException

class EmbeddedEntityTest extends BaseTestSuite {

  test("Construct embedded node") {
    val given =
      EmbeddedNode("n" -> "id").build
        .withImpliedLabel("Person")
        .withOptionalLabel("Employee" -> "is_emp")
        .withProperty("name")
        .withProperty("age" -> "YEARS")
        .withProperty("age" -> "AGE")

    val actual = EmbeddedNode(
      "n",
      "id",
      Map("Person" -> None, "Employee" -> Some("is_emp")),
      Map("name" -> Set("name"), "age" -> Set("YEARS", "AGE"))
    )

    given should equal(actual)
    show(given) should equal("Seq((AGE,n.age :: ?), (YEARS,n.age :: ?), (id,n :: :Person NODE), (is_emp,n:Employee :: BOOLEAN), (name,n.name :: ?))")
  }

  test("Construct embedded relationship with static type") {
    val given =
      EmbeddedRelationship("r").from("src").relType("KNOWS").to("dst").build
        .withProperty("name")
        .withProperty("age" -> "YEARS")
        .withProperty("age" -> "AGE")

    val actual = EmbeddedRelationship(
      "r",
      "r",
      "src",
      Right("KNOWS"),
      "dst",
      Map("name" -> Set("name"), "age" -> Set("YEARS", "AGE"))
    )

    given should equal(actual)
    show(given) should equal("Seq((AGE,r.age :: ?), (YEARS,r.age :: ?), (dst,target(r :: :KNOWS RELATIONSHIP)), (name,r.name :: ?), (r,r :: :KNOWS RELATIONSHIP), (src,source(r :: :KNOWS RELATIONSHIP)))")
  }

  test("Construct embedded relationship with dynamic type") {
    val given =
      EmbeddedRelationship("r").from("src").relTypes("typ", "ADMIRES", "IGNORES").to("dst").build
        .withProperty("name")
        .withProperty("age" -> "YEARS")
        .withProperty("age" -> "AGE")

    val actual = EmbeddedRelationship(
      "r",
      "r",
      "src",
      Left("typ" -> Set("ADMIRES", "IGNORES")),
      "dst",
      Map("name" -> Set("name"), "age" -> Set("YEARS", "AGE"))
    )

    given should equal(actual)
    show(given) should equal("Seq((AGE,r.age :: ?), (YEARS,r.age :: ?), (dst,target(r :: :ADMIRES|IGNORES RELATIONSHIP)), (name,r.name :: ?), (r,r :: :ADMIRES|IGNORES RELATIONSHIP), (src,source(r :: :ADMIRES|IGNORES RELATIONSHIP)), (typ,type(r) :: INTEGER))")
  }

  test("Refuses to use the same slot multiple times when constructing nodes") {
    raisesSlotReUse(EmbeddedNode("n" -> "the_slot").build.withOptionalLabel("Person" -> "the_slot").verify)
    raisesSlotReUse(EmbeddedNode("n" -> "the_slot").build.withProperty("a" -> "the_slot").verify)
  }

  test("Refuses to use the same slot multiple times when constructing relationships") {
    raisesSlotReUse(EmbeddedRelationship("r").from("r").to("b").relType("KNOWS").build.verify)
    raisesSlotReUse(EmbeddedRelationship("r").from("a").to("r").relType("KNOWS").build.verify)
    raisesSlotReUse(EmbeddedRelationship("r").from("a").to("b").relTypes("r", "KNOWS").build.verify)
    raisesSlotReUse(EmbeddedRelationship("r" -> "the_slot").from("a").to("b").relType("KNOWS").build.withProperty("a" -> "the_slot").verify)
  }

  private def show(entity: VerifiedEmbeddedEntity[_]) = {
    val slots = entity.slots

    slots.keys.toSeq.sorted.map(k => k -> slots(k)).mkString("Seq(", ", ", ")")
  }

  private def raisesSlotReUse[T](f: => T): Unit = {
    an[SparkCypherException] should be thrownBy f
  }
}
