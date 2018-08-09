/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.api.schema

import org.opencypher.okapi.api.types._
import org.scalatest.{FunSpec, Matchers}

class SchemaTest extends FunSpec with Matchers {

  it("lists of void and others") {
    val s1 = Schema.empty.withNodePropertyKeys("A")("v" -> CTList(CTVoid))
    val s2 = Schema.empty.withNodePropertyKeys("A")("v" -> CTList(CTString).nullable)

    val joined = s1 ++ s2
    joined should equal(s2)
  }

  it("should provide all labels") {
    Schema.empty.withNodePropertyKeys("Person")().labels should equal(Set("Person"))
  }

  it("should provide all types") {
    Schema.empty
      .withRelationshipPropertyKeys("KNOWS")()
      .withRelationshipPropertyKeys("HAS")()
      .relationshipTypes should equal(Set("KNOWS", "HAS"))
  }

  it("should give correct node property schema") {
    val schema = Schema.empty.withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger)

    schema.nodeKeys("NotPerson") shouldBe empty
    schema.nodeKeys("Person") should equal(Map("name" -> CTString, "age" -> CTInteger))
    schema.labels should equal(Set("Person"))
  }

  it("should give correct relationship property schema") {
    val schema = Schema.empty.withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)

    schema.relationshipKeys("NOT_KNOWS") shouldBe empty
    schema.relationshipKeys("KNOWS") should equal(Map("since" -> CTInteger, "relative" -> CTBoolean))
    schema.relationshipTypes should equal(Set("KNOWS"))
  }

  it("should get simple implication correct") {
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

  it("should get chained implications correct") {
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

  it("should get chained combinations correct") {
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

  it("should get simple combinations correct") {
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

  it("chaining calls should amend types") {
    val schema = Schema.empty
      .withNodePropertyKeys("Foo")("name" -> CTString)
      .withNodePropertyKeys("Foo")("name" -> CTString, "age" -> CTInteger)
      .withRelationshipPropertyKeys("BAR")("p1" -> CTBoolean)
      .withRelationshipPropertyKeys("BAR")("p2" -> CTFloat)

    schema.nodeKeys("Foo") should equal(Map("name" -> CTString, "age" -> CTInteger.nullable))
    schema.relationshipKeys("BAR") should equal(Map("p1" -> CTBoolean.nullable, "p2" -> CTFloat.nullable))
  }

  it("combining schemas, separate keys") {
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

  it("combining schemas, key subset") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString)
    val schema2 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString, "baz" -> CTString)

    schema1 ++ schema2 should equal(
      Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString, "baz" -> CTString.nullable))
  }

  it("combining schemas, partial key overlap") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString)
    val schema2 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "baz" -> CTString)

    schema1 ++ schema2 should equal(
      Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString.nullable, "baz" -> CTString.nullable))
  }

  it("combining type conflicting schemas should work across nullability") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString.nullable, "bar" -> CTString)
    val schema2 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString.nullable)

    schema1 ++ schema2 should equal(
      Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString.nullable, "bar" -> CTString.nullable))
  }

  it("combining schemas with restricting label implications") {
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

  it("extract node schema") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee", "Person")("name" -> CTString, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("notName" -> CTBoolean)
      .withRelationshipPropertyKeys("OWNER")("since" -> CTInteger)

    schema.forNode(Set("Person")) should equal(
      Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString)
        .withNodePropertyKeys("Employee", "Person")("name" -> CTString, "salary" -> CTInteger)
    )

    schema.forNode(Set("Dog")) should equal(
      Schema.empty
        .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
    )

    schema.forNode(Set("Dog", "Pet")) should equal(
      Schema.empty
        .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
    )
  }

  it("forRelationship") {
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

  it("handles empty label set") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set.empty[String], Map("name" -> CTString))
      .withNodePropertyKeys("A")("name" -> CTInteger)

    schema.nodeKeys() should equal(Map("name" -> CTString))
    schema.nodeKeys(Set.empty[String]) should equal(Map("name" -> CTString))
  }

  it("get node key type with all given semantics") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set("A"), Map("a" -> CTInteger, "b" -> CTString, "c" -> CTFloat, "d" -> CTFloat.nullable))
      .withNodePropertyKeys(Set.empty[String], Map("a" -> CTString))

    schema.nodeKeyType(Set("A"), "a") should equal(Some(CTInteger))
    schema.nodeKeyType(Set.empty[String], "a") should equal(Some(CTAny))
    schema.nodeKeyType(Set.empty[String], "b") should equal(Some(CTString.nullable))
    schema.nodeKeyType(Set("B"), "b") should equal(None)
    schema.nodeKeyType(Set("A"), "x") should equal(None)
  }

  it("get rel key type") {
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

  it("get all keys") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set.empty[String], Map("a" -> CTString, "c" -> CTString, "d" -> CTString.nullable, "f" -> CTString))
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

  it("get keys for") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set.empty[String], Map("a" -> CTString, "c" -> CTString, "d" -> CTString.nullable, "f" -> CTString))
      .withNodePropertyKeys("A")("b" -> CTInteger, "c" -> CTString, "e" -> CTString, "f" -> CTInteger)
      .withNodePropertyKeys("B")("b" -> CTFloat, "c" -> CTString, "e" -> CTInteger)

    schema.keysFor("A") should equal(Map("b" -> CTInteger, "c" -> CTString, "e" -> CTString, "f" -> CTInteger))
    schema.keysFor("B") should equal(Map("b" -> CTFloat, "c" -> CTString, "e" -> CTInteger))
    schema.keysFor("A", "B") should equal(Map("b" -> CTNumber, "c" -> CTString, "e" -> CTAny, "f" -> CTInteger.nullable))
  }

  it("get keys for label combinations") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set.empty[String], Map("a" -> CTString, "c" -> CTString, "d" -> CTString.nullable, "f" -> CTString))
      .withNodePropertyKeys("A")("b" -> CTInteger, "c" -> CTString, "e" -> CTString, "f" -> CTInteger)
      .withNodePropertyKeys("B")("b" -> CTFloat, "c" -> CTString, "e" -> CTInteger)

    schema.keysFor(Set(Set("A"))) should equal(Map("b" -> CTInteger, "c" -> CTString, "e" -> CTString, "f" -> CTInteger))
    schema.keysFor(Set(Set("B"))) should equal(Map("b" -> CTFloat, "c" -> CTString, "e" -> CTInteger))
    schema.keysFor(Set(Set("A"), Set("B"))) should equal(Map("b" -> CTNumber, "c" -> CTString, "e" -> CTAny, "f" -> CTInteger.nullable))
    schema.keysFor(Set(Set("A", "B"))) should equal(Map.empty)
    schema.keysFor(Set(Set.empty[String])) should equal(Map("a" -> CTString, "c" -> CTString, "d" -> CTString.nullable, "f" -> CTString))
  }

  it("isEmpty") {
    Schema.empty.isEmpty shouldBe true
    (Schema.empty ++ Schema.empty).isEmpty shouldBe true
    val empty = Schema.empty
    empty.isEmpty shouldBe true
    (empty ++ Schema.empty).isEmpty shouldBe true

    Schema.empty.withNodePropertyKeys("label")().isEmpty shouldBe false
    Schema.empty.withRelationshipPropertyKeys("type")("name" -> CTFloat).isEmpty shouldBe false
  }

  it("should serialize and deserialize a schema") {

    val schema = Schema.empty
      .withNodePropertyKeys(Set("A"), PropertyKeys("foo" -> CTString, "bar" -> CTList(CTString.nullable)))
      .withNodePropertyKeys(Set("A", "B"), PropertyKeys("foo" -> CTString, "bar" -> CTInteger))
      .withRelationshipPropertyKeys("FOO", PropertyKeys.empty)

    val serialized = schema.toJson

    schema should equal(Schema.fromJson(serialized))

  }

  it("concatenating schemas should make missing relationship properties nullable") {
    val schema1 = Schema.empty
      .withRelationshipPropertyKeys("FOO")()

    val schema2 = Schema.empty
      .withRelationshipPropertyKeys("FOO")("p" -> CTString)

    val schemaSum = schema1 ++ schema2

    schemaSum should equal(
      Schema.empty
        .withRelationshipPropertyKeys("FOO")("p" -> CTString.nullable)
    )
  }

  it("concatenating schemas should make missing node properties nullable") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("Foo")()

    val schema2 = Schema.empty
      .withNodePropertyKeys("Foo")("p" -> CTString)

    val schemaSum = schema1 ++ schema2

    schemaSum should equal(
      Schema.empty
        .withNodePropertyKeys("Foo")("p" -> CTString.nullable)
    )
  }

  it("serializes to/from json") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("FOO")("p" -> CTString)
      .withNodePropertyKeys("BAR")("q" -> CTInteger)

    val serialized = schema.toJson

    serialized should equal(
      """|{
         |    "version": 1,
         |    "labelPropertyMap": [
         |        {
         |            "labels": [
         |                "BAR"
         |            ],
         |            "properties": {
         |                "q": "INTEGER"
         |            }
         |        }
         |    ],
         |    "relTypePropertyMap": [
         |        {
         |            "relType": "FOO",
         |            "properties": {
         |                "p": "STRING"
         |            }
         |        }
         |    ]
         |}""".stripMargin)

    val deserialized = Schema.fromJson(serialized)

    deserialized should equal(schema)
  }

}
