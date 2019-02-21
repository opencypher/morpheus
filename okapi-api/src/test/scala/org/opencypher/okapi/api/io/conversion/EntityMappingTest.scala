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
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException

class EntityMappingTest extends ApiBaseTest {
  describe("NodeMappingBuilder") {
    it("Construct node mapping") {
      val given = NodeMappingBuilder.on("id")
        .withImpliedLabel("Person")
        .withPropertyKey("name")
        .withPropertyKey("age" -> "YEARS").build


      val pattern = NodePattern(CTNode("Person"))
      val expected = EntityMapping(
        NodePattern(CTNode("Person")),
        Map(
          pattern.nodeEntity -> Map("name" -> "name", "age" -> "YEARS")
        ),
        Map(
          pattern.nodeEntity -> Map(SourceIdKey -> "id")
        )
      )

      given should equal(expected)
    }

    it("Refuses to overwrite a property with a different mapping") {
      raisesIllegalArgument(NodeMappingBuilder.on("sourceKey").withPropertyKey("a" -> "foo").withPropertyKey("a" -> "bar").build)
    }
  }

  describe("RelationshipMappingBuilder") {
    it("Construct relationship mapping with static type") {
      val given = RelationshipMappingBuilder.on("r")
        .from("src")
        .to("dst")
        .relType("KNOWS")
        .withPropertyKey("name")
        .withPropertyKey("age" -> "YEARS").build

      val pattern = RelationshipPattern(CTRelationship("KNOWS"))
      val actual = EntityMapping(
        pattern,
        Map(
          pattern.relEntity -> Map("name" -> "name", "age" -> "YEARS")
        ),
        Map(
          pattern.relEntity -> Map(SourceIdKey -> "r", SourceStartNodeKey -> "src", SourceEndNodeKey -> "dst")
        )
      )

      given should equal(actual)
    }

    it("Refuses to overwrite a property with a different mapping") {
      raisesIllegalArgument(
        RelationshipMappingBuilder
          .on("sourceKey")
          .from("a")
          .to("b")
          .relType("KNOWS")
          .withPropertyKey("a" -> "foo").withPropertyKey("a" -> "bar")
          .build
      )
    }

    it("Refuses to use the same source key for incompatible types when constructing relationships") {
      raisesIllegalArgument(RelationshipMappingBuilder.on("r").from("r").to("b").relType("KNOWS").build)
      raisesIllegalArgument(RelationshipMappingBuilder.on("r").from("a").to("r").relType("KNOWS").build)
    }
  }

  describe("validation") {
    it("throws an error if relationship entities do not have exactly one type") {
      val pattern1 = RelationshipPattern(CTRelationship("Foo", "Bar"))
      raisesIllegalArgument(EntityMapping.empty(pattern1))

      val pattern2 = RelationshipPattern(CTRelationship())
      raisesIllegalArgument(EntityMapping.empty(pattern2))
    }
  }

  private def raisesIllegalArgument[T](f: => T): Unit = {
    an[IllegalArgumentException] should be thrownBy f
  }
}
