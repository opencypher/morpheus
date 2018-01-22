/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.api.schema

import org.opencypher.caps.api.exception.SchemaException
import org.opencypher.caps.api.schema.Schema.{AllLabels, NoLabel}
import org.opencypher.caps.api.types._
import org.opencypher.caps.test.BaseTestSuite

class SchemaTest extends BaseTestSuite {

  test("lists of void and others") {
    val s1 = Schema.empty.withNodePropertyKeys("A")("v" -> CTList(CTVoid))
    val s2 = Schema.empty.withNodePropertyKeys("A")("v" -> CTList(CTString).nullable)

    val joined = s1 ++ s2
    joined should equal(s2)
    joined.verify // should not throw
  }

  test("should provide all labels") {
    Schema.empty.withNodePropertyKeys("Person")().labels should equal(Set("Person"))
  }

  test("should provide all types") {
    Schema.empty
      .withRelationshipPropertyKeys("KNOWS")()
      .withRelationshipPropertyKeys("HAS")()
      .relationshipTypes should equal(Set("KNOWS", "HAS"))
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
    val schema = Schema.empty
      .withNodePropertyKeys("Foo", "Bar")("prop" -> CTBoolean)
      .withNodePropertyKeys("Person", "Employee")("name" -> CTString, "nbr" -> CTInteger)
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Person", "Dog")("name" -> CTString, "reg" -> CTFloat)
      .withNodePropertyKeys("Dog")("reg" -> CTFloat)

    schema.impliedLabels("Person") shouldBe Set("Person")
    schema.impliedLabels("Employee") shouldBe Set("Person", "Employee")
    schema.impliedLabels("Employee", "Person") shouldBe Set("Person", "Employee")
    schema.impliedLabels("Foo") shouldBe Set("Foo", "Bar")
    schema.impliedLabels("Bar") shouldBe Set("Foo", "Bar")
    schema.impliedLabels("Dog") shouldBe Set("Dog")
    schema.labels should equal(Set("Person", "Employee", "Foo", "Bar", "Dog"))
  }

  test("should get chained implications correct") {
    val schema = Schema.empty
      .withNodePropertyKeys("Employee", "Person", "Someone", "Human")()
      .withNodePropertyKeys("Person", "Someone", "Human")()
      .withNodePropertyKeys("Human")()
      .withNodePropertyKeys("Someone")()

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
    val schema = Schema.empty
      .withNodePropertyKeys("Person", "Employee")()
      .withNodePropertyKeys("Person", "Director")()
      .withNodePropertyKeys("Employee", "Director")()

    schema.combinationsFor(Set("Employee")) should equal(Set(Set("Person", "Employee"), Set("Employee", "Director")))
    schema.combinationsFor(Set("Director")) should equal(Set(Set("Person", "Director"), Set("Employee", "Director")))
    schema.combinationsFor(Set("Person")) should equal(Set(Set("Person", "Employee"), Set("Person", "Director")))
    schema.combinationsFor(Set("Person", "Employee")) should equal(Set(Set("Person", "Employee")))
    schema.labels should equal(Set("Person", "Employee", "Director"))
  }

  test("should get simple combinations correct") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person", "Employee")()
      .withNodePropertyKeys("Dog", "Pet")()

    schema.combinationsFor(Set("NotEmployee")) should equal(Set())
    schema.combinationsFor(Set("Employee")) should equal(Set(Set("Person", "Employee")))
    schema.combinationsFor(Set("Person")) should equal(Set(Set("Person", "Employee")))
    schema.combinationsFor(Set("Dog")) should equal(Set(Set("Dog", "Pet")))
    schema.combinationsFor(Set("Pet", "Employee")) should equal(Set())
    schema.labels should equal(Set("Person", "Employee", "Dog", "Pet"))
  }

  test("verifying empty schema") {
    Schema.empty.verify should equal(Schema.empty)
  }

  test("verifying valid schema") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("notName" -> CTBoolean)

    schema.verify should equal(schema)
  }

  test("verifying schema with conflict on implied labels") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee", "Person")("name" -> CTString, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("name" -> CTBoolean)

    the[SchemaException] thrownBy schema.verify should have message
      "The property types FLOAT and BOOLEAN (for property 'name' and label 'Pet') can not be stored in the same Spark column"
  }

  test("verifying schema with conflict on combined labels") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee", "Person")("name" -> CTInteger, "salary" -> CTInteger)
      .withNodePropertyKeys("Employee")("name" -> CTInteger, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("notName" -> CTBoolean)

    the[SchemaException] thrownBy schema.verify should have message
      "The property types STRING and INTEGER (for property 'name' and label 'Person') can not be stored in the same Spark column"
  }

  test("chaining calls should amend types") {
    val schema = Schema.empty
      .withNodePropertyKeys("Foo")("name" -> CTString)
      .withNodePropertyKeys("Foo")("name" -> CTString, "age" -> CTInteger)
      .withRelationshipPropertyKeys("BAR")("p1" -> CTBoolean)
      .withRelationshipPropertyKeys("BAR")("p2" -> CTFloat)

    schema.nodeKeys("Foo") should equal(Map("name" -> CTString, "age" -> CTInteger.nullable))
    schema.relationshipKeys("BAR") should equal(Map("p1" -> CTBoolean.nullable, "p2" -> CTFloat.nullable))
  }

  test("should not allow updates to conflict with existing types") {
    val schema = Schema.empty.withNodePropertyKeys("Foo")("name" -> CTString)

    schema.withNodePropertyKeys("Foo")("name" -> CTString).verify // same type: fine
    schema.withNodePropertyKeys("Foo2")("name" -> CTInteger).verify // different label: fine
    schema.withNodePropertyKeys("Foo")("name2" -> CTInteger).verify // different key: fine
    the[SchemaException] thrownBy {
      schema.withNodePropertyKeys("Foo")("name" -> CTInteger).verify // not fine
    } should have message "The property types INTEGER and STRING (for property 'name') can not be stored in the same Spark column"
  }

  test("combining schemas, separate keys") {
    val schema1 = Schema.empty.withNodePropertyKeys("A")("foo" -> CTString)
    val schema2 = Schema.empty.withNodePropertyKeys("B")("bar" -> CTString)
    val schema3 = Schema.empty
      .withNodePropertyKeys("C")("baz" -> CTString)
      .withNodePropertyKeys("A", "C")("baz" -> CTString)
      .withNodePropertyKeys("A", "C", "X")("baz" -> CTString)

    schema1 ++ schema2 ++ schema3 should equal(
      Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString)
        .withNodePropertyKeys("B")("bar" -> CTString)
        .withNodePropertyKeys("C")("baz" -> CTString)
        .withNodePropertyKeys("A", "C")("baz" -> CTString)
        .withNodePropertyKeys("A", "C", "X")("baz" -> CTString))
  }

  test("combining schemas, key subset") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString)
    val schema2 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString, "baz" -> CTString)

    schema1 ++ schema2 should equal(
      Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString, "baz" -> CTString.nullable))
  }

  test("combining schemas, partial key overlap") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString)
    val schema2 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "baz" -> CTString)

    schema1 ++ schema2 should equal(
      Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString.nullable, "baz" -> CTString.nullable))
  }

  test("combining type conflicting schemas should work across nullability") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString.nullable, "bar" -> CTString)
    val schema2 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString.nullable)

    schema1 ++ schema2 should equal(
      Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString.nullable, "bar" -> CTString.nullable))
  }

  test("combining type conflicting schemas should fail, ANY") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString)
    val schema2 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTInteger)

    the[SchemaException] thrownBy (schema1 ++ schema2) should have message
      "The property types INTEGER and STRING (for property 'bar') can not be stored in the same Spark column"
  }

  // TODO: Do we want to support this particular join between property keys? (Number)
  test("combining type conflicting schemas should fail, NUMBER") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "baz" -> CTInteger)
    val schema2 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "baz" -> CTFloat)

    the[SchemaException] thrownBy (schema1 ++ schema2) should have message
      "The property types FLOAT and INTEGER (for property 'baz') can not be stored in the same Spark column"
  }

  test("combining schemas with restricting label implications") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A", "B", "C")()
      .withNodePropertyKeys("B", "C")()
      .withNodePropertyKeys("A", "E", "B", "C")()
    val schema2 = Schema.empty
      .withNodePropertyKeys("B", "C", "D")()
      .withNodePropertyKeys("C", "D")()
      .withNodePropertyKeys("B", "F", "C", "D")()

    schema1 ++ schema2 should equal(
      Schema.empty
        .withNodePropertyKeys("A", "B", "C")()
        .withNodePropertyKeys("B", "C")()
        .withNodePropertyKeys("A", "E", "B", "C")()
        .withNodePropertyKeys("B", "C", "D")()
        .withNodePropertyKeys("C", "D")()
        .withNodePropertyKeys("B", "F", "C", "D")())
  }

  test("extract node schema") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee", "Person")("name" -> CTString, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("notName" -> CTBoolean)
      .withRelationshipPropertyKeys("OWNER")("since" -> CTInteger)

    schema.forNode(CTNode("Person")) should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString)
        .withNodePropertyKeys("Employee", "Person")("name" -> CTString, "salary" -> CTInteger)
    )

    schema.forNode(CTNode("Dog")) should equal(
      Schema.empty
        .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
        .withNodePropertyKeys("Pet")("notName" -> CTBoolean)
    )
  }

  test("forRelationship") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Person", "Employee")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTString)
      .withRelationshipPropertyKeys("KNOWS")("name" -> CTString)
      .withRelationshipPropertyKeys("LOVES")("deeply" -> CTBoolean, "salary" -> CTInteger)
      .withRelationshipPropertyKeys("NEEDS")("rating" -> CTFloat)
      .withNodePropertyKeys("Dog", "Pet")()
      .withNodePropertyKeys("Pet")()
      .withRelationshipPropertyKeys("OWNER")("since" -> CTInteger)

    schema.forRelationship(CTRelationship("KNOWS")) should equal(
      Schema.empty
        .withRelationshipPropertyKeys("KNOWS")("name" -> CTString)
    )

    schema.forRelationship(CTRelationship) should equal(
      Schema.empty
        .withRelationshipPropertyKeys("KNOWS")("name" -> CTString)
        .withRelationshipPropertyKeys("LOVES")("deeply" -> CTBoolean, "salary" -> CTInteger)
        .withRelationshipPropertyKeys("NEEDS")("rating" -> CTFloat)
        .withRelationshipPropertyKeys("OWNER")("since" -> CTInteger)
    )

    schema.forRelationship(CTRelationship("KNOWS", "LOVES")) should equal(
      Schema.empty
        .withRelationshipPropertyKeys("KNOWS")("name" -> CTString)
        .withRelationshipPropertyKeys("LOVES")("deeply" -> CTBoolean, "salary" -> CTInteger)
    )
  }

  test("handles empty label set") {
    val schema = Schema.empty
      .withNodePropertyKeys(NoLabel, Map("name" -> CTString))
      .withNodePropertyKeys("A")("name" -> CTInteger)

    schema.nodeKeys() should equal(Map("name" -> CTString))
    schema.nodeKeys(NoLabel) should equal(Map("name" -> CTString))
  }

  test("get node key type with all given semantics") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set("A"), Map("a" -> CTInteger, "b" -> CTString, "c" -> CTFloat, "d" -> CTFloat.nullable))
      .withNodePropertyKeys(NoLabel, Map("a" -> CTString))

    schema.nodeKeyType(AllOf("A"), "a") should equal(Some(CTInteger))
    schema.nodeKeyType(AllGiven(NoLabel), "a") should equal(Some(CTAny))
    schema.nodeKeyType(AllGiven(NoLabel), "b") should equal(Some(CTString.nullable))
    schema.nodeKeyType(AllOf("B"), "b") should equal(None)
    schema.nodeKeyType(AllOf("A"), "x") should equal(None)
  }

  test("get node key type with any given semantics") {
    val schema = Schema.empty
      .withNodePropertyKeys(
        Set("A", "B"),
        Map("a" -> CTInteger, "b" -> CTString, "c" -> CTFloat, "d" -> CTFloat.nullable))
      .withNodePropertyKeys(NoLabel, Map("a" -> CTString))

    schema.nodeKeyType(AnyOf("A"), "a") should equal(Some(CTInteger))
    schema.nodeKeyType(AnyGiven(NoLabel), "a") should equal(Some(CTString))
    schema.nodeKeyType(AnyOf("A"), "b") should equal(Some(CTString))
    schema.nodeKeyType(AnyGiven(AllLabels), "a") should equal(Some(CTAny))
  }

  test("get rel key type") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("A")("a" -> CTInteger, "b" -> CTString, "c" -> CTFloat, "d" -> CTFloat.nullable)
      .withRelationshipPropertyKeys("B")(
        "a" -> CTFloat,
        "b" -> CTString.nullable,
        "c" -> CTString
      )
      .withRelationshipType("C")

    schema.relationshipKeyType(Set("A"), "a") should equal(Some(CTInteger))
    schema.relationshipKeyType(Set("A", "B"), "a") should equal(Some(CTNumber))
    schema.relationshipKeyType(Set("A", "B"), "b") should equal(Some(CTString.nullable))
    schema.relationshipKeyType(Set("A", "B", "C"), "c") should equal(Some(CTAny.nullable))
    schema.relationshipKeyType(Set("A"), "e") should equal(None)

    schema.relationshipKeyType(Set.empty, "a") should equal(Some(CTNumber.nullable))
  }

  test("get all keys") {
    val schema = Schema.empty
      .withNodePropertyKeys(NoLabel, Map("a" -> CTString, "c" -> CTString, "d" -> CTString.nullable, "f" -> CTString))
      .withNodePropertyKeys("A")("b" -> CTInteger, "c" -> CTString, "e" -> CTString, "f" -> CTInteger)
      .withNodePropertyKeys("B")("b" -> CTFloat, "c" -> CTString, "e" -> CTInteger, "f" -> CTBoolean)

    schema.allNodeKeys should equal(
      Map(
        "a" -> CTString.nullable,
        "b" -> CTNumber.nullable,
        "c" -> CTString,
        "d" -> CTString.nullable,
        "e" -> CTAny.nullable,
        "f" -> CTAny))
  }

  test("isEmpty") {
    Schema.empty.isEmpty shouldBe true
    (Schema.empty ++ Schema.empty).isEmpty shouldBe true
    val empty = Schema(
      Set.empty,
      Set.empty,
      LabelPropertyMap.empty,
      RelTypePropertyMap.empty
    )
    empty.isEmpty shouldBe true
    (empty ++ Schema.empty).isEmpty shouldBe true

    Schema.empty.withNodePropertyKeys("label")().isEmpty shouldBe false
    Schema.empty.withRelationshipPropertyKeys("type")("name" -> CTFloat).isEmpty shouldBe false
  }

  test("can not use empty labels") {
    the[SchemaException] thrownBy Schema.empty.withNodePropertyKeys("")() should have message
      "Labels must be non-empty"
  }
}
