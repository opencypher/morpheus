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
package org.opencypher.caps.ir.impl

import org.opencypher.caps.api.schema.{PropertyKeys, Schema}
import org.opencypher.caps.api.types._
import org.opencypher.caps.ir.api.IRField
import org.opencypher.caps.ir.api.pattern.{DirectedRelationship, Pattern}
import org.opencypher.caps.test.BaseTestSuite

class RichSchemaTest extends BaseTestSuite {
    describe("fromPattern") {
      it("can convert a pattern with all known fields") {
        val schema = Schema.empty
          .withNodePropertyKeys("Person")("name" -> CTString)
          .withNodePropertyKeys("City")("name" -> CTString, "region" -> CTBoolean)
          .withRelationshipPropertyKeys("KNOWS")("since" -> CTFloat.nullable)
          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)

        val actual = schema.forPattern(Pattern(
          Set(
            IRField("n")(CTNode("Person")),
            IRField("r")(CTRelationship("BAR")),
            IRField("m")(CTNode("Person"))
          ),
          Map(
            IRField("r")(CTRelationship("BAR")) -> DirectedRelationship(IRField("n")(CTNode("Person")), IRField("m")(CTNode("Person")))
          )
        ))

        val expected = Schema.empty
          .withNodePropertyKeys("Person")("name" -> CTString)
          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)

        actual should be(expected)
      }

      it("can convert a pattern with unknown field") {
        val schema = Schema.empty
          .withNodePropertyKeys("Person")("name" -> CTString)
          .withNodePropertyKeys("City")("name" -> CTString, "region" -> CTBoolean)
          .withRelationshipPropertyKeys("KNOWS")("since" -> CTFloat.nullable)
          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)

        val actual = schema.forPattern(Pattern(
          Set(
            IRField("n")(CTNode("Person")),
            IRField("r")(CTRelationship("BAR")),
            IRField("m")(CTNode())
          ),
          Map(
            IRField("r")(CTRelationship("BAR")) -> DirectedRelationship(IRField("n")(CTNode("Person")), IRField("m")(CTNode("Person")))
          )
        ))

        val expected = Schema.empty
          .withNodePropertyKeys("Person")("name" -> CTString)
          .withNodePropertyKeys(Set.empty[String], PropertyKeys.empty)
          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)

        actual should be(expected)
      }

      it("can convert a pattern with empty labeled field") {
        val schema = Schema.empty
          .withNodePropertyKeys("Person")("name" -> CTString)
          .withNodePropertyKeys("City")("name" -> CTString, "region" -> CTBoolean)
          .withNodePropertyKeys()("name" -> CTString)
          .withRelationshipPropertyKeys("KNOWS")("since" -> CTFloat.nullable)
          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)

        val actual = schema.forPattern(Pattern(
          Set(
            IRField("n")(CTNode("Person")),
            IRField("r")(CTRelationship("BAR")),
            IRField("m")(CTNode())
          ),
          Map(
            IRField("r")(CTRelationship("BAR")) -> DirectedRelationship(IRField("n")(CTNode("Person")), IRField("m")(CTNode("Person")))
          )
        ))

        val expected = Schema.empty
          .withNodePropertyKeys("Person")("name" -> CTString)
          .withNodePropertyKeys()("name" -> CTString)
          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)

        actual should be(expected)
      }
    }

}
