/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.opencypher.okapi.ApiBaseTest
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.impl.util.Version
import org.scalatest.{FunSpec, Matchers}

class SchemaTest extends ApiBaseTest {

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

    schema.nodePropertyKeys(Set("NotPerson")) shouldBe empty
    schema.nodePropertyKeys(Set("Person")) should equal(Map("name" -> CTString, "age" -> CTInteger))
    schema.labels should equal(Set("Person"))
  }

  it("should give correct relationship property schema") {
    val schema = Schema.empty.withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)

    schema.relationshipPropertyKeys("NOT_KNOWS") shouldBe empty
    schema.relationshipPropertyKeys("KNOWS") should equal(Map("since" -> CTInteger, "relative" -> CTBoolean))
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

    schema.nodePropertyKeys(Set("Foo")) should equal(Map("name" -> CTString, "age" -> CTInteger.nullable))
    schema.relationshipPropertyKeys("BAR") should equal(Map("p1" -> CTBoolean.nullable, "p2" -> CTFloat.nullable))
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

    schema.nodePropertyKeys(Set.empty) should equal(Map("name" -> CTString))
    schema.nodePropertyKeys(Set.empty[String]) should equal(Map("name" -> CTString))
  }

  it("get node key type with all given semantics") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set("A"), Map("a" -> CTInteger, "b" -> CTString, "c" -> CTFloat, "d" -> CTFloat.nullable))
      .withNodePropertyKeys(Set.empty[String], Map("a" -> CTString))

    schema.nodePropertyKeyType(Set("A"), "a") should equal(Some(CTInteger))
    schema.nodePropertyKeyType(Set.empty[String], "a") should equal(Some(CTUnion(CTString, CTInteger)))
    schema.nodePropertyKeyType(Set.empty[String], "b") should equal(Some(CTString.nullable))
    schema.nodePropertyKeyType(Set("B"), "b") should equal(None)
    schema.nodePropertyKeyType(Set("A"), "x") should equal(None)
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

    schema.relationshipPropertyKeyType(Set("A"), "a") should equal(Some(CTInteger))
    schema.relationshipPropertyKeyType(Set("A", "B"), "a") should equal(Some(CTNumber))
    schema.relationshipPropertyKeyType(Set("A", "B"), "b") should equal(Some(CTString.nullable))
    schema.relationshipPropertyKeyType(Set("A", "B", "C"), "c") should equal(Some(CTUnion(CTFloat, CTString).nullable))
    schema.relationshipPropertyKeyType(Set("A"), "e") should equal(None)

    schema.relationshipPropertyKeyType(Set.empty, "a") should equal(Some(CTNumber.nullable))
  }

  it("get all keys") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set.empty[String], Map("a" -> CTString, "c" -> CTString, "d" -> CTString.nullable, "f" -> CTString))
      .withNodePropertyKeys("A")("b" -> CTInteger, "c" -> CTString, "e" -> CTString, "f" -> CTInteger)
      .withNodePropertyKeys("B")("b" -> CTFloat, "c" -> CTString, "e" -> CTInteger, "f" -> CTBoolean)

    allNodePropertyKeys(schema) should equal(
      Map(
        "a" -> CTString.nullable,
        "b" -> CTNumber.nullable,
        "c" -> CTString,
        "d" -> CTString.nullable,
        "e" -> CTUnion(CTString, CTInteger).nullable,
        "f" -> CTUnion(CTString, CTInteger, CTTrue, CTFalse)))
  }

  it("get keys for") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set.empty[String], Map("a" -> CTString, "c" -> CTString, "d" -> CTString.nullable, "f" -> CTString))
      .withNodePropertyKeys("A")("b" -> CTInteger, "c" -> CTString, "e" -> CTString, "f" -> CTInteger)
      .withNodePropertyKeys("B")("b" -> CTFloat, "c" -> CTString, "e" -> CTInteger)

    schema.nodePropertyKeysForCombinations(Set(Set("A"))) should equal(Map("b" -> CTInteger, "c" -> CTString, "e" -> CTString, "f" -> CTInteger))
    schema.nodePropertyKeysForCombinations(Set(Set("B"))) should equal(Map("b" -> CTFloat, "c" -> CTString, "e" -> CTInteger))
    schema.nodePropertyKeysForCombinations(Set(Set("A"), Set("B"))) should equal(Map("b" -> CTNumber, "c" -> CTString, "e" -> CTUnion(CTString, CTInteger), "f" -> CTInteger.nullable))
  }

  it("get keys for label combinations") {
    val schema = Schema.empty
      .withNodePropertyKeys(Set.empty[String], Map("a" -> CTString, "c" -> CTString, "d" -> CTString.nullable, "f" -> CTString))
      .withNodePropertyKeys("A")("b" -> CTInteger, "c" -> CTString, "e" -> CTString, "f" -> CTInteger)
      .withNodePropertyKeys("B")("b" -> CTFloat, "c" -> CTString, "e" -> CTInteger)

    schema.nodePropertyKeysForCombinations(Set(Set("A"))) should equal(Map("b" -> CTInteger, "c" -> CTString, "e" -> CTString, "f" -> CTInteger))
    schema.nodePropertyKeysForCombinations(Set(Set("B"))) should equal(Map("b" -> CTFloat, "c" -> CTString, "e" -> CTInteger))
    schema.nodePropertyKeysForCombinations(Set(Set("A"), Set("B"))) should equal(Map("b" -> CTNumber, "c" -> CTString, "e" -> CTUnion(CTString, CTInteger), "f" -> CTInteger.nullable))
    schema.nodePropertyKeysForCombinations(Set(Set("A", "B"))) should equal(Map.empty)
    schema.nodePropertyKeysForCombinations(Set(Set.empty[String])) should equal(Map("a" -> CTString, "c" -> CTString, "d" -> CTString.nullable, "f" -> CTString))
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

  it("concatenates explicit schema patterns") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("Foo")()
      .withRelationshipPropertyKeys("REL")()
      .withSchemaPatterns(SchemaPattern("Foo", "REL", "Foo"))

    val schema2 = Schema.empty
      .withNodePropertyKeys("Bar")()
      .withRelationshipPropertyKeys("REL")()
      .withSchemaPatterns(SchemaPattern("Bar", "REL", "Bar"))

    val schemaSum = schema1 ++ schema2

    schemaSum should equal(
      Schema.empty
        .withNodePropertyKeys("Foo")()
        .withRelationshipPropertyKeys("REL")()
        .withNodePropertyKeys("Bar")()
        .withRelationshipPropertyKeys("REL")()
        .withSchemaPatterns(SchemaPattern("Foo", "REL", "Foo"))
        .withSchemaPatterns(SchemaPattern("Bar", "REL", "Bar"))
    )
  }

  it("serializes to/from json when patterns and keys are not present") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("FOO")("p" -> CTString)
      .withNodePropertyKeys("BAR")("q" -> CTInteger)

    val serialized = schema.toJson

    serialized should equal(
      """|{
         |    "version": "1.0",
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

  it("serializes to/from json when patterns and keys are present") {
    val schema = Schema.empty
      .withRelationshipPropertyKeys("FOO")("p" -> CTString)
      .withNodePropertyKeys("BAR")("q" -> CTInteger)
      .withSchemaPatterns(SchemaPattern(Set("BAR"), "FOO", Set("BAR")))
      .withNodeKey("BAR", Set("q"))
      .withRelationshipKey("FOO", Set("p"))

    val serialized = schema.toJson

    serialized should equal(
      """|{
         |    "version": "1.0",
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
         |    ],
         |    "schemaPatterns": [
         |        {
         |            "sourceLabelCombination": [
         |                "BAR"
         |            ],
         |            "relType": "FOO",
         |            "targetLabelCombination": [
         |                "BAR"
         |            ]
         |        }
         |    ],
         |    "nodeKeys": {
         |        "BAR": [
         |            "q"
         |        ]
         |    },
         |    "relKeys": {
         |        "FOO": [
         |            "p"
         |        ]
         |    }
         |}""".stripMargin)

    val deserialized = Schema.fromJson(serialized)

    deserialized should equal(schema)
  }

  describe("pattern schemas") {
    it("is empty if the schema is empty") {
      val schema = Schema.empty
      schema.schemaPatterns shouldBe empty
    }

    it("is empty if there are no relationships") {
      val schema = Schema.empty.withNodePropertyKeys("A")()
      schema.schemaPatterns shouldBe empty
    }

    it("is empty if there are no nodes") {
      val schema = Schema.empty.withRelationshipPropertyKeys("A")()
      schema.schemaPatterns shouldBe empty
    }

    it("gives all the inferred patterns") {
      val schema = Schema.empty
        .withNodePropertyKeys("A")()
        .withNodePropertyKeys("B")()
        .withRelationshipPropertyKeys("REL")()

      schema.schemaPatterns should equal(Set(
        SchemaPattern(Set("A"), "REL", Set("A")),
        SchemaPattern(Set("A"), "REL", Set("B")),
        SchemaPattern(Set("B"), "REL", Set("A")),
        SchemaPattern(Set("B"), "REL", Set("B"))
      ))
    }

    it("returns the explicit patterns if any were given") {
      val schema = Schema.empty
        .withNodePropertyKeys("A")()
        .withNodePropertyKeys("B")()
        .withRelationshipPropertyKeys("REL")()
        .withSchemaPatterns(SchemaPattern("A", "REL", "B"))

      schema.schemaPatterns should equal(Set(
        SchemaPattern("A", "REL", "B")
      ))
    }

    it("throws a SchemaException when adding a schema pattern into an empty schema") {
      a[SchemaException] should be thrownBy {
        Schema.empty.withSchemaPatterns(SchemaPattern("A", "REL", "B"))
      }
    }

    it("throws a SchemaException when adding a schema pattern for unknown start node labels") {
      a[SchemaException] should be thrownBy {
        Schema.empty
          .withNodePropertyKeys("A")()
          .withRelationshipPropertyKeys("REL")()
          .withSchemaPatterns(SchemaPattern("A", "REL", "B"))
      }
    }

    it("throws a SchemaException when adding a schema pattern for unknown end node labels") {
      a[SchemaException] should be thrownBy {
        Schema.empty
          .withNodePropertyKeys("B")()
          .withRelationshipPropertyKeys("REL")()
          .withSchemaPatterns(SchemaPattern("A", "REL", "B"))
      }
    }

    it("throws a SchemaException when adding a schema pattern for unknown rel type") {
      a[SchemaException] should be thrownBy {
        Schema.empty
          .withNodePropertyKeys("A")()
          .withNodePropertyKeys("B")()
          .withSchemaPatterns(SchemaPattern("A", "REL", "B"))
      }
    }
  }

  describe("schemaPatternsFor") {
    val aRel1B = SchemaPattern("A", "REL1", "B")
    val aRel2CD = SchemaPattern(Set("A"), "REL2", Set("C", "D"))
    val bRel2CD = SchemaPattern(Set("B"), "REL2", Set("C", "D"))
    val CDRel1A = SchemaPattern(Set("C", "D"), "REL1", Set("A"))
    val emptyRel1Empty = SchemaPattern(Set.empty[String], "REL1", Set.empty[String])

    val schema = Schema.empty
      .withNodePropertyKeys("A")()
      .withNodePropertyKeys("B")()
      .withNodePropertyKeys("C", "D")()
      .withNodePropertyKeys()()
      .withRelationshipPropertyKeys("REL1")()
      .withRelationshipPropertyKeys("REL2")()
      .withSchemaPatterns(aRel1B)
      .withSchemaPatterns(aRel2CD)
      .withSchemaPatterns(bRel2CD)
      .withSchemaPatterns(CDRel1A)
      .withSchemaPatterns(emptyRel1Empty)

    it("works when nothing is known") {
      schema.schemaPatternsFor(Set.empty, Set.empty, Set.empty) should equal(Set(
        aRel1B,
        aRel2CD,
        bRel2CD,
        CDRel1A,
        emptyRel1Empty
      ))
    }

    it("works when only the source node label is known") {
      schema.schemaPatternsFor(Set.empty, Set.empty, Set.empty) should equal(Set(
        aRel1B,
        aRel2CD,
        bRel2CD,
        CDRel1A,
        emptyRel1Empty
      ))

      schema.schemaPatternsFor(Set("A"), Set.empty, Set.empty) should equal(Set(
        aRel1B,
        aRel2CD
      ))

      schema.schemaPatternsFor(Set("C"), Set.empty, Set.empty) should equal(Set(
        CDRel1A
      ))
    }

    it("works when only the target node label is known") {
      schema.schemaPatternsFor(Set.empty, Set.empty, Set("A")) should equal(Set(
        CDRel1A
      ))

      schema.schemaPatternsFor(Set.empty, Set.empty, Set("C")) should equal(Set(
        aRel2CD,
        bRel2CD
      ))
    }

    it("works when only the rel type is known") {
      schema.schemaPatternsFor(Set.empty, Set("REL1"), Set.empty) should equal(Set(
        aRel1B,
        CDRel1A,
        emptyRel1Empty
      ))

      schema.schemaPatternsFor(Set.empty, Set("REL1", "REL2"), Set.empty) should equal(Set(
        aRel1B,
        aRel2CD,
        bRel2CD,
        CDRel1A,
        emptyRel1Empty
      ))
    }

    it("works when every thing is known") {
      schema.schemaPatternsFor(Set("A"), Set("REL1"), Set("B")) should equal(Set(
        aRel1B
      ))

      schema.schemaPatternsFor(Set("A"), Set("REL2"), Set("C")) should equal(Set(
        aRel2CD
      ))

      schema.schemaPatternsFor(Set("A"), Set("REL1"), Set("C")) shouldBe empty
    }

    it("works for no existing labels/types") {
      schema.schemaPatternsFor(Set("A", "B"), Set.empty, Set.empty) shouldBe empty
      schema.schemaPatternsFor(Set.empty, Set.empty, Set("A", "B")) shouldBe empty
      schema.schemaPatternsFor(Set.empty, Set("REL3"), Set.empty) shouldBe empty
    }
  }

  describe("entity keys") {
    it("adds node keys") {
      val schema = Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString)
        .withNodeKey("A", Set("foo", "bar"))
      schema.nodeKeys shouldEqual Map("A" -> Set("foo", "bar"))
    }

    it("adds relationship keys") {
      val schema = Schema.empty
        .withRelationshipPropertyKeys("A")("foo" -> CTString, "bar" -> CTString)
        .withRelationshipKey("A", Set("foo", "bar"))
      schema.relationshipKeys shouldEqual Map("A" -> Set("foo", "bar"))
    }

    it("fails to add unknown entity keys") {
      a[SchemaException] shouldBe thrownBy {
        Schema.empty.withNodeKey("A", Set.empty)
      }
      a[SchemaException] shouldBe thrownBy {
        Schema.empty.withRelationshipKey("A", Set.empty)
      }
    }

    it("merges overlapping keys during schema merge") {
      val schema1 = Schema.empty
        .withNodePropertyKeys("A")("foo" -> CTString)
        .withNodeKey("A", Set("foo"))
      val schema2 = Schema.empty
        .withNodePropertyKeys("A")("bar" -> CTString)
        .withNodeKey("A", Set("bar"))
      val joinedSchema = schema1 ++ schema2

      joinedSchema.nodeKeys shouldEqual Map("A" -> Set("foo", "bar"))
    }

    it("fails if a node key refers to a non-existing label") {
      an[SchemaException] shouldBe thrownBy {
        Schema.empty
          .withNodePropertyKeys("A")("foo" -> CTString)
          .withNodeKey("B", Set("foo"))
      }
    }

    it("fails if a node key refers to a non-existing property key for the label") {
      an[SchemaException] shouldBe thrownBy {
        Schema.empty
          .withNodePropertyKeys("A")("foo" -> CTString)
          .withNodeKey("A", Set("bar"))
      }
    }

    it("fails if a node key refers to a nullable property key for the label") {
      an[SchemaException] shouldBe thrownBy {
        Schema.empty
          .withNodePropertyKeys("A")("foo" -> CTString.nullable)
          .withNodeKey("A", Set("foo"))
      }
    }


    it("fails if a relationship key refers to a non-existing label") {
      an[SchemaException] shouldBe thrownBy {
        Schema.empty
          .withRelationshipPropertyKeys("A")("foo" -> CTString)
          .withRelationshipKey("B", Set("foo"))
      }
    }

    it("fails if a relationship key refers to a non-existing property key for the label") {
      an[SchemaException] shouldBe thrownBy {
        Schema.empty
          .withRelationshipPropertyKeys("A")("foo" -> CTString)
          .withRelationshipKey("A", Set("bar"))
      }
    }

    it("fails if a relationship key refers to a nullable property key for the label") {
      an[SchemaException] shouldBe thrownBy {
        Schema.empty
          .withRelationshipPropertyKeys("A")("foo" -> CTString.nullable)
          .withRelationshipKey("A", Set("foo"))
      }
    }
  }

  describe("version") {
    def schemaJson(version: Version): String =
      s"""|{
          |    "version": "$version",
          |    "labelPropertyMap": [],
          |    "relTypePropertyMap": []
          |}""".stripMargin

    it("it allows to read compatible schema versions from string") {
      Seq(
        Schema.CURRENT_VERSION.major.toString,
        s"${Schema.CURRENT_VERSION.major}.0",
        s"${Schema.CURRENT_VERSION.major}.5"
      ).foreach { v =>
        Schema.fromJson(schemaJson(Version(v)))
      }
    }

    it("raises an error when the version is incompatible") {
      Seq(
        (Schema.CURRENT_VERSION.major + 1).toString,
        s"${Schema.CURRENT_VERSION.major + 1}.0",
        s"${Schema.CURRENT_VERSION.major - 1}.5"
      ).foreach { v =>
        an[SchemaException] shouldBe thrownBy {
          Schema.fromJson(schemaJson(Version(v)))
        }
      }
    }
  }

  private def allNodePropertyKeys(schema: Schema): PropertyKeys = {
    val keyToTypes = schema.allCombinations
      .map(schema.nodePropertyKeys)
      .toSeq
      .flatten
      .groupBy(_._1)
      .map {
        case (k, v) => k -> v.map(_._2)
      }

    keyToTypes
      .mapValues(types => types.foldLeft[CypherType](CTVoid)(_ join _))
      .map {
        case (key, tpe) =>
          if (schema.allCombinations.map(schema.nodePropertyKeys).forall(_.get(key).isDefined))
            key -> tpe
          else key -> tpe.nullable
      }
  }

}
