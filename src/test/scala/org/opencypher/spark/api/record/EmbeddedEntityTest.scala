package org.opencypher.spark.api.record

import org.opencypher.spark.TestSuiteImpl

class EmbeddedEntityTest extends TestSuiteImpl {

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
  }

//  test("Refuses to use the same slot multiple times when constructing nodes") {
//    raisesSlotReUse(EmbeddedNode("n" -> "the_slot").build.withOptionalLabel("Person" -> "the_slot"))
//  }
//
//  private def raisesSlotReUse[T](f: => T): Unit = {
//    an[IllegalArgumentException] should be thrownBy(f)
//  }
}
