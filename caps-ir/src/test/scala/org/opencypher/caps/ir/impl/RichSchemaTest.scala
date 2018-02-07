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

import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types._
import org.opencypher.caps.ir.api.IRField
import org.opencypher.caps.test.BaseTestSuite

class RichSchemaTest extends BaseTestSuite {

//    test("for entities") {
//      val schema = Schema.empty
//        .withNodePropertyKeys("Person")("name" -> CTString)
//        .withNodePropertyKeys("City")("name" -> CTString, "region" -> CTBoolean)
//        .withRelationshipPropertyKeys("KNOWS")("since" -> CTFloat.nullable)
//        .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger)
//
//      schema.forEntities(
//        Set(
//          IRField("n")(CTNode("Person")),
//          IRField("r")(CTRelationship("BAR"))
//        )) should equal(
//        Schema.empty
//          .withNodePropertyKeys("Person")("name" -> CTString)
//          .withRelationshipPropertyKeys("BAR")("foo" -> CTInteger))
//    }

}
