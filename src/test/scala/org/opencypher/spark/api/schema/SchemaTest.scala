/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.spark.api.schema

import org.opencypher.spark.BaseTestSuite
import org.opencypher.spark.api.types.{CTBoolean, CTFloat, CTInteger, CTString}

class SchemaTest extends BaseTestSuite {

  test("should provide all labels") {
    Schema.empty.withNodePropertyKeys("Person")().labels should equal(Set("Person"))
  }

  test("should provide all types") {
    Schema.empty.withRelationshipPropertyKeys("KNOWS")().withRelationshipPropertyKeys("HAS")().relationshipTypes should equal(Set("KNOWS", "HAS"))
  }

  test("should give correct node property schema") {
    val schema = Schema.empty.withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger)

    schema.nodeKeys("NotPerson") shouldBe empty
    schema.nodeKeys("Person") should equal(Map("name" -> CTString, "age" -> CTInteger))
    schema.labels should equal(Set("Person"))
  }

  test("should give correct relationship property schema") {
    val schema = Schema.empty.withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)

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
    val schema = Schema.empty.withLabelCombination("Person", "Employee").withLabelCombination("Person", "Director")

    schema.labelCombination(Set("Employee")) should equal(Set("Person", "Employee", "Director"))
    schema.labelCombination(Set("Director")) should equal(Set("Person", "Employee", "Director"))
    schema.labelCombination(Set("Person")) should equal(Set("Person", "Employee", "Director"))
    schema.labelCombination(Set("Person", "Employee")) should equal(Set("Person", "Employee", "Director"))
    schema.labels should equal(Set("Person", "Employee", "Director"))
  }

  test("should get simple combinations correct") {
    val schema = Schema.empty.withLabelCombination("Person", "Employee").withLabelCombination("Dog", "Pet")

    schema.labelCombination(Set("NotEmployee")) should equal(Set())
    schema.labelCombination(Set("Employee")) should equal(Set("Person", "Employee"))
    schema.labelCombination(Set("Person")) should equal(Set("Person", "Employee"))
    schema.labelCombination(Set("Dog")) should equal(Set("Dog", "Pet"))
    schema.labelCombination(Set("Pet", "Employee")) should equal(Set("Person", "Employee", "Dog", "Pet"))
    schema.labels should equal(Set("Person", "Employee", "Dog", "Pet"))
  }

  test("verifying empty schema") {
    Schema.empty.verify.schema should equal(Schema.empty)
  }

  test("verifying valid schema") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("notName" -> CTBoolean)
      .withLabelCombination("Person", "Employee")
      .withImpliedLabel("Dog", "Pet")

    schema.verify.schema should equal(schema)
  }

  test("verifying schema with conflict on implied labels") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("name" -> CTBoolean)
      .withLabelCombination("Person", "Employee")
      .withImpliedLabel("Dog", "Pet")

    an [IllegalArgumentException] shouldBe thrownBy {
      schema.verify
    }
  }

  test("verifying schema with conflict on combined labels") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTInteger, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("notName" -> CTBoolean)
      .withLabelCombination("Person", "Employee")
      .withImpliedLabel("Dog", "Pet")

    an [IllegalArgumentException] shouldBe thrownBy {
      schema.verify
    }
  }

  test("chaining calls should work") {
    val schema = Schema.empty
      .withNodePropertyKeys("Foo")("name" -> CTString)
      .withNodePropertyKeys("Foo")("age" -> CTInteger)
      .withRelationshipPropertyKeys("BAR")("p1" -> CTBoolean)
      .withRelationshipPropertyKeys("BAR")("p2" -> CTFloat)

    schema.nodeKeys("Foo") should equal(Map("name" -> CTString, "age" -> CTInteger))
    schema.relationshipKeys("BAR") should equal(Map("p1" -> CTBoolean, "p2" -> CTFloat))
  }

  test("should not allow updates to conflict with existing types") {
    val schema = Schema.empty.withNodePropertyKeys("Foo")("name" -> CTString)

    schema.withNodePropertyKeys("Foo")("name" -> CTString).verify    // same type: fine
    schema.withNodePropertyKeys("Foo2")("name" -> CTInteger).verify  // different label: fine
    schema.withNodePropertyKeys("Foo")("name2" -> CTInteger).verify  // different key: fine
    an [IllegalStateException] shouldBe thrownBy {
      schema.withNodePropertyKeys("Foo")("name" -> CTInteger).verify // not fine
    }
  }

  test("combining non-conflicting schemas") {
    val schema1 = Schema.empty.withNodePropertyKeys("A")("foo" -> CTString)
    val schema2 = Schema.empty.withNodePropertyKeys("B")("bar" -> CTString)
    val schema3 = Schema.empty.withNodePropertyKeys("C")("baz" -> CTString)

    schema1 ++ schema2 ++ schema3 should equal(Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString)
      .withNodePropertyKeys("B")("bar" -> CTString)
      .withNodePropertyKeys("C")("baz" -> CTString))
  }

  test("combining non-conflicting schemas with implied labels") {
    val schema1 = Schema.empty.withImpliedLabel("A", "B")
      .withNodePropertyKeys("A")("foo" -> CTString)
      .withNodePropertyKeys("B")("bar" -> CTString)
    val schema2 = Schema.empty.withNodePropertyKeys("B")("bar" -> CTString)

    schema1 ++ schema2 should equal(Schema.empty
      .withImpliedLabel("A", "B")
      .withNodePropertyKeys("A")("foo" -> CTString)
      .withNodePropertyKeys("B")("bar" -> CTString))
   }

  test("combining schemas with restricting label implications") {
    val schema1 = Schema.empty
      .withImpliedLabel("A", "B")
      .withImpliedLabel("B", "C")
      .withLabelCombination("A","E")
      .withNodePropertyKeys("A")()
      .withNodePropertyKeys("B")()
      .withNodePropertyKeys("C")()
      .withNodePropertyKeys("E")()
    val schema2 = Schema.empty
      .withImpliedLabel("B", "C")
      .withImpliedLabel("C", "D")
      .withLabelCombination("B","F")
      .withNodePropertyKeys("B")()
      .withNodePropertyKeys("C")()
      .withNodePropertyKeys("D")()
      .withNodePropertyKeys("F")()


    schema1 ++ schema2 should equal(Schema.empty
      .withImpliedLabel("A", "B")
      .withImpliedLabel("B", "C")
      .withLabelCombination("A","E")
      .withLabelCombination("B","F")
      .withLabelCombination("C","D")
      .withNodePropertyKeys("A")()
      .withNodePropertyKeys("B")()
      .withNodePropertyKeys("C")()
      .withNodePropertyKeys("D")()
      .withNodePropertyKeys("E")()
      .withNodePropertyKeys("F")())
  }
}
