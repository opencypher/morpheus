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
package org.opencypher.okapi.ir.impl

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.ir.api.IRField
import org.opencypher.okapi.ir.api.pattern.{DirectedRelationship, Pattern}
import org.opencypher.okapi.testing.BaseTestSuite

import scala.collection.immutable.ListMap

class RichSchemaTest extends BaseTestSuite {
    describe("fromFields") {
      it("can convert fields in a pattern") {
        val schema = Schema.empty
          .withNodePropertyKeys("Person")("name" -> CTString)
          .withNodePropertyKeys("City")("name" -> CTString, "region" -> CTBoolean)
          .withRelationshipPropertyKeys("KNOWS")("since" -> CTFloat.nullable)
          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)

        val actual = Pattern(
          Set(
            IRField("n")(CTNode("Person")),
            IRField("r")(CTRelationship("BAR")),
            IRField("m")(CTNode("Person"))
          ),
          ListMap(
            IRField("r")(CTRelationship("BAR")) -> DirectedRelationship(IRField("n")(CTNode("Person")), IRField("m")(CTNode("Person")))
          )
        ).fields.map(f => schema.forElementType(f.cypherType)).reduce(_ ++ _)

        val expected = Schema.empty
          .withNodePropertyKeys("Person")("name" -> CTString)
          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)

        actual should be(expected)
      }

      it("can compute a schema when a field is unknown") {
        val schema = Schema.empty
          .withNodePropertyKeys("Person")("name" -> CTString)
          .withNodePropertyKeys("City")("name" -> CTString, "region" -> CTBoolean)
          .withRelationshipPropertyKeys("KNOWS")("since" -> CTFloat.nullable)
          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)

        val actual = Pattern(
          Set(
            IRField("n")(CTNode("Person")),
            IRField("r")(CTRelationship("BAR")),
            IRField("m")(CTNode())
          ),
          ListMap(
            IRField("r")(CTRelationship("BAR")) -> DirectedRelationship(IRField("n")(CTNode("Person")), IRField("m")(CTNode()))
          )
        ).fields.map(f => schema.forElementType(f.cypherType)).reduce(_ ++ _)

        val expected = Schema.empty
          .withNodePropertyKeys("Person")("name" -> CTString)
          .withNodePropertyKeys("City")("name" -> CTString, "region" -> CTBoolean)
          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)

        actual should be(expected)
      }
    }
}
