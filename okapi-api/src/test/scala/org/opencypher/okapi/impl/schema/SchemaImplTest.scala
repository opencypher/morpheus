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
package org.opencypher.okapi.impl.schema

import org.opencypher.okapi.api.schema.{PropertyKeys, Schema}
import org.opencypher.okapi.api.types.{CTInteger, CTList, CTString}
import org.scalatest.{FunSpec, Matchers}
import SchemaImpl._

class SchemaImplTest extends FunSpec with Matchers {

  it("should write a schema") {

    val schema = Schema.empty
      .withNodePropertyKeys(Set("A"), PropertyKeys("foo" -> CTString, "bar" -> CTList(CTString.nullable)))
      .withNodePropertyKeys(Set("A", "B"), PropertyKeys("foo" -> CTString, "bar" -> CTInteger))
      .asInstanceOf[SchemaImpl]

    val serialized = upickle.default.write(schema, indent = 4)

    val deserializedSchema = upickle.default.read(serialized)

    schema should equal(deserializedSchema)

  }

}
