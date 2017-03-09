package org.opencypher.spark.prototype.api.schema

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTInteger, CTString}

class StdSchemaTest extends StdTestSuite {

  test("should provide all labels") {
    StdSchema.empty.withNodeKeys("Person")().labels should equal(Set("Person"))
  }

  test("should provide all types") {
    StdSchema.empty.withRelationshipKeys("KNOWS")().withRelationshipKeys("HAS")().relationshipTypes should equal(Set("KNOWS", "HAS"))
  }

  test("should give correct node property schema") {
    val schema = StdSchema.empty.withNodeKeys("Person")("name" -> CTString, "age" -> CTInteger)

    schema.nodeKeys("NotPerson") shouldBe empty
    schema.nodeKeys("Person") should equal(Map("name" -> CTString, "age" -> CTInteger))
    schema.labels should equal(Set("Person"))
  }

  test("should give correct relationship property schema") {
    val schema = StdSchema.empty.withRelationshipKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)

    schema.relationshipKeys("NOT_KNOWS") shouldBe empty
    schema.relationshipKeys("KNOWS") should equal(Map("since" -> CTInteger, "relative" -> CTBoolean))
    schema.relationshipTypes should equal(Set("KNOWS"))
  }

  test("should get simple implication correct") {
    val schema = StdSchema.empty.withImpliedLabel("Employee", "Person")

    schema.impliedLabels(Set("Person")) shouldBe Set("Person")
    schema.impliedLabels(Set("Employee")) shouldBe Set("Person", "Employee")
    schema.impliedLabels(Set("Employee", "Person")) shouldBe Set("Person", "Employee")
    schema.labels should equal(Set("Person", "Employee"))
  }

  test("should get chained implications correct") {
    val schema = StdSchema.empty.withImpliedLabel("Employee", "Person")
      .withImpliedLabel("Person", "Human")
      .withImpliedLabel("Person", "Someone")

    schema.impliedLabels(Set("Unknown")) shouldBe empty
    schema.impliedLabels(Set("Unknown", "Person")) shouldBe Set("Person", "Human", "Someone")
    schema.impliedLabels(Set("Human")) shouldBe Set("Human")
    schema.impliedLabels(Set("Someone")) shouldBe Set("Someone")
    schema.impliedLabels(Set("Person")) shouldBe Set("Person", "Human", "Someone")
    schema.impliedLabels(Set("Person", "Human")) shouldBe Set("Person", "Human", "Someone")
    schema.impliedLabels(Set("Person", "Someone")) shouldBe Set("Person", "Human", "Someone")
    schema.impliedLabels(Set("Employee")) shouldBe Set("Employee", "Person", "Human", "Someone")
    schema.impliedLabels(Set("Employee", "Person")) shouldBe Set("Employee", "Person", "Human", "Someone")
    schema.labels should equal(Set("Person", "Employee", "Human", "Someone"))
  }

  test("should get chained combinations correct") {
    val schema = StdSchema.empty.withCombinedLabels("Person", "Employee").withCombinedLabels("Person", "Director")

    schema.combinedLabels(Set("Employee")) should equal(Set("Person", "Employee", "Director"))
    schema.combinedLabels(Set("Director")) should equal(Set("Person", "Employee", "Director"))
    schema.combinedLabels(Set("Person")) should equal(Set("Person", "Employee", "Director"))
    schema.combinedLabels(Set("Person", "Employee")) should equal(Set("Person", "Employee", "Director"))
    schema.labels should equal(Set("Person", "Employee", "Director"))
  }

  test("should get simple combinations correct") {
    val schema = StdSchema.empty.withCombinedLabels("Person", "Employee").withCombinedLabels("Dog", "Pet")

    schema.combinedLabels(Set("NotEmployee")) should equal(Set())
    schema.combinedLabels(Set("Employee")) should equal(Set("Person", "Employee"))
    schema.combinedLabels(Set("Person")) should equal(Set("Person", "Employee"))
    schema.combinedLabels(Set("Dog")) should equal(Set("Dog", "Pet"))
    schema.combinedLabels(Set("Pet", "Employee")) should equal(Set("Person", "Employee", "Dog", "Pet"))
    schema.labels should equal(Set("Person", "Employee", "Dog", "Pet"))
  }
}
