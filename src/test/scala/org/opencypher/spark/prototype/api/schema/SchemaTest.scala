package org.opencypher.spark.prototype.api.schema

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.prototype.api.types.{CTBoolean, CTFloat, CTInteger, CTString}

class SchemaTest extends StdTestSuite {

  test("should provide all labels") {
    Schema.empty.withNodeKeys("Person")().labels should equal(Set("Person"))
  }

  test("should provide all types") {
    Schema.empty.withRelationshipKeys("KNOWS")().withRelationshipKeys("HAS")().relationshipTypes should equal(Set("KNOWS", "HAS"))
  }

  test("should give correct node property schema") {
    val schema = Schema.empty.withNodeKeys("Person")("name" -> CTString, "age" -> CTInteger)

    schema.nodeKeys("NotPerson") shouldBe empty
    schema.nodeKeys("Person") should equal(Map("name" -> CTString, "age" -> CTInteger))
    schema.labels should equal(Set("Person"))
  }

  test("should give correct relationship property schema") {
    val schema = Schema.empty.withRelationshipKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)

    schema.relationshipKeys("NOT_KNOWS") shouldBe empty
    schema.relationshipKeys("KNOWS") should equal(Map("since" -> CTInteger, "relative" -> CTBoolean))
    schema.relationshipTypes should equal(Set("KNOWS"))
  }

  test("should get simple implication correct") {
    val schema = Schema.empty.withImpliedLabel("Employee", "Person")

    schema.impliedLabels(Set("Person")) shouldBe Set("Person")
    schema.impliedLabels(Set("Employee")) shouldBe Set("Person", "Employee")
    schema.impliedLabels(Set("Employee", "Person")) shouldBe Set("Person", "Employee")
    schema.labels should equal(Set("Person", "Employee"))
  }

  test("should get chained implications correct") {
    val schema = Schema.empty.withImpliedLabel("Employee", "Person")
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
    val schema = Schema.empty.withCombinedLabels("Person", "Employee").withCombinedLabels("Person", "Director")

    schema.optionalLabels(Set("Employee")) should equal(Set("Person", "Employee", "Director"))
    schema.optionalLabels(Set("Director")) should equal(Set("Person", "Employee", "Director"))
    schema.optionalLabels(Set("Person")) should equal(Set("Person", "Employee", "Director"))
    schema.optionalLabels(Set("Person", "Employee")) should equal(Set("Person", "Employee", "Director"))
    schema.labels should equal(Set("Person", "Employee", "Director"))
  }

  test("should get simple combinations correct") {
    val schema = Schema.empty.withCombinedLabels("Person", "Employee").withCombinedLabels("Dog", "Pet")

    schema.optionalLabels(Set("NotEmployee")) should equal(Set())
    schema.optionalLabels(Set("Employee")) should equal(Set("Person", "Employee"))
    schema.optionalLabels(Set("Person")) should equal(Set("Person", "Employee"))
    schema.optionalLabels(Set("Dog")) should equal(Set("Dog", "Pet"))
    schema.optionalLabels(Set("Pet", "Employee")) should equal(Set("Person", "Employee", "Dog", "Pet"))
    schema.labels should equal(Set("Person", "Employee", "Dog", "Pet"))
  }

  test("verifying empty schema") {
    Schema.empty.verify.schema should equal(Schema.empty)
  }

  test("verifying valid schema") {
    val schema = Schema.empty
      .withNodeKeys("Person")("name" -> CTString)
      .withNodeKeys("Employee")("name" -> CTString, "salary" -> CTInteger)
      .withNodeKeys("Dog")("name" -> CTFloat)
      .withNodeKeys("Pet")("notName" -> CTBoolean)
      .withCombinedLabels("Person", "Employee")
      .withImpliedLabel("Dog", "Pet")

    schema.verify.schema should equal(schema)
  }

  test("verifying schema with conflict on implied labels") {
    val schema = Schema.empty
      .withNodeKeys("Person")("name" -> CTString)
      .withNodeKeys("Employee")("name" -> CTString, "salary" -> CTInteger)
      .withNodeKeys("Dog")("name" -> CTFloat)
      .withNodeKeys("Pet")("name" -> CTBoolean)
      .withCombinedLabels("Person", "Employee")
      .withImpliedLabel("Dog", "Pet")

    an [IllegalArgumentException] shouldBe thrownBy {
      schema.verify
    }
  }

  test("verifying schema with conflict on combined labels") {
    val schema = Schema.empty
      .withNodeKeys("Person")("name" -> CTString)
      .withNodeKeys("Employee")("name" -> CTInteger, "salary" -> CTInteger)
      .withNodeKeys("Dog")("name" -> CTFloat)
      .withNodeKeys("Pet")("notName" -> CTBoolean)
      .withCombinedLabels("Person", "Employee")
      .withImpliedLabel("Dog", "Pet")

    an [IllegalArgumentException] shouldBe thrownBy {
      schema.verify
    }
  }

  test("chaining calls should work") {
    val schema = Schema.empty
      .withNodeKeys("Foo")("name" -> CTString)
      .withNodeKeys("Foo")("age" -> CTInteger)
      .withRelationshipKeys("BAR")("p1" -> CTBoolean)
      .withRelationshipKeys("BAR")("p2" -> CTFloat)

    schema.nodeKeys("Foo") should equal(Map("name" -> CTString, "age" -> CTInteger))
    schema.relationshipKeys("BAR") should equal(Map("p1" -> CTBoolean, "p2" -> CTFloat))
  }

  test("should not allow updates to conflict with existing types") {
    val schema = Schema.empty.withNodeKeys("Foo")("name" -> CTString)

    schema.withNodeKeys("Foo")("name" -> CTString).verify    // same type: fine
    schema.withNodeKeys("Foo2")("name" -> CTInteger).verify  // different label: fine
    schema.withNodeKeys("Foo")("name2" -> CTInteger).verify  // different key: fine
    an [IllegalStateException] shouldBe thrownBy {
      schema.withNodeKeys("Foo")("name" -> CTInteger).verify // not fine
    }
  }
}
