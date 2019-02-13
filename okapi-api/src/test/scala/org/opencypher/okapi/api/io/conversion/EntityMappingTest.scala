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
package org.opencypher.okapi.api.io.conversion

import org.opencypher.okapi.ApiBaseTest
import org.opencypher.okapi.impl.exception.IllegalArgumentException

class EntityMappingTest extends ApiBaseTest {

  it("Construct node mapping") {
    val given = NodeMapping.on("id")
      .withImpliedLabel("Person")
      .withOptionalLabel("Employee" -> "is_emp")
      .withPropertyKey("name")
      .withPropertyKey("age" -> "YEARS")

    val actual = NodeMapping(
      "id",
      Set("Person"),
      Map("Employee" -> "is_emp"),
      Map("name" -> "name", "age" -> "YEARS"))

    given should equal(actual)
  }

  it("Construct relationship mapping with static type") {
    val given = RelationshipMapping.on("r")
      .from("src")
      .to("dst")
      .relType("KNOWS")
      .withPropertyKey("name")
      .withPropertyKey("age" -> "YEARS")

    val actual = RelationshipMapping(
      "r",
      "src",
      "dst",
      Left("KNOWS"),
      Map("name" -> "name", "age" -> "YEARS")
    )

    given should equal(actual)
  }

  it("Construct relationship mapping with dynamic type") {
    val given = RelationshipMapping.on("r")
      .from("src")
      .to("dst")
      .withSourceRelTypeKey("KNOWS", Set("ADMIRES", "IGNORES"))
      .withPropertyKey("name")
      .withPropertyKey("age" -> "YEARS")

    val actual = RelationshipMapping(
      "r",
      "src",
      "dst",
      Right("KNOWS" -> Set("ADMIRES", "IGNORES")),
      Map("name" -> "name", "age" -> "YEARS")
    )

    given should equal(actual)
  }

  it("Refuses to use the same source key for incompatible types when constructing node mappings") {
    raisesIllegalArgument(NodeMapping.on("sourceKey").withOptionalLabel("Person" -> "sourceKey"))
  }

  it("Refuses to overwrite a property with a different mapping") {
    raisesIllegalArgument(NodeMapping.on("sourceKey").withPropertyKey("a" -> "foo").withPropertyKey("a" -> "bar"))
    raisesIllegalArgument(RelationshipMapping.on("sourceKey").from("a").to("b").relType("KNOWS")
      .withPropertyKey("a" -> "foo").withPropertyKey("a" -> "bar"))
  }

  // TODO reenable
  ignore("Refuses to use the same source key for incompatible types when constructing relationships") {
    raisesIllegalArgument(RelationshipMapping.on("r").from("r").to("b").relType("KNOWS"))
    raisesIllegalArgument(RelationshipMapping.on("r").from("a").to("r").relType("KNOWS"))
    raisesIllegalArgument(RelationshipMapping.on("r").from("a").to("b").withSourceRelTypeKey("r", Set("KNOWS")))
    raisesIllegalArgument(RelationshipMapping.on("r").from("a").to("b").withSourceRelTypeKey("a", Set("KNOWS")))
    raisesIllegalArgument(RelationshipMapping.on("r").from("a").to("b").withSourceRelTypeKey("b", Set("KNOWS")))
  }

  private def raisesIllegalArgument[T](f: => T): Unit = {
    an[IllegalArgumentException] should be thrownBy f
  }
}
