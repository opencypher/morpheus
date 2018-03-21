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

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.schema.TagSupport._
import org.opencypher.okapi.test.BaseTestSuite

class TagSupportTest extends BaseTestSuite {

  it("allows adding tags to a schema") {
    val taggedSchema = Schema.empty.withTags(1, 2, 3)
    taggedSchema.tags should equal(Set(1, 2, 3))
  }

  it("preserves other schema entries when adding tags") {
    val taggedSchema = Schema.empty.withNodePropertyKeys(Set("a")).withTags(1, 2, 3)
    taggedSchema.tags should equal(Set(1, 2, 3))
    taggedSchema.labels should equal(Set("a"))
  }

  it("merges tags when concatenating schemas") {
    val s1: Schema with TagSupport = Schema.empty.withTags(1, 2, 3)
    val s2: Schema with TagSupport = Schema.empty.withTags(1, 2, 4)
    val concatenatedSchema = s1 ++ s2
    concatenatedSchema should equal(Schema.empty.withTags(1, 2, 3, 4))
  }

  it("replaces tags") {
    val taggedSchema = Schema.empty.withTags(1, 2, 3).replaceTags(Map(2 -> 5))
    taggedSchema should equal(Schema.empty.withTags(1, 5, 3))
  }

  it("replaces tags on the rhs schema when resolving tag conflicts in a schema union") {
    val s1: Schema with TagSupport = Schema.empty.withTags(1, 2, 3)
    val s2: Schema with TagSupport = Schema.empty.withTags(1, 2, 4)
    val schemaUnion = s1 union s2
    schemaUnion.tags.size should equal(6)
    Set(1, 2, 3).subsetOf(schemaUnion.tags) should equal(true)
  }

}
