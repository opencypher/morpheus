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
package org.opencypher.spark.impl.physical

import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.types.CypherType.{CTNode, CTRelationship, CTString}
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords}
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.testing.CAPSTestSuite

class RecordHeaderMismatch extends CAPSTestSuite {

  it("throws a schema exception when the physical record header does not match the one computed based on the schema") {
    val buggyGraph = new CAPSGraph {

      override def schema: CAPSSchema = Schema.empty
        .withNodePropertyKeys("A")("name" -> CTString)
        .withRelationshipPropertyKeys("R")("name" -> CTString)
        .asCaps

      // Always return empty records, which does not match what the schema promises
      override def nodes(name: String, nodeCypherType: CypherType): CAPSRecords = CAPSRecords.empty()(caps)

      override implicit def session: CAPSSession = caps

      override def tags: Set[Int] = Set(0)

      override def relationships(name: String, relCypherType: CypherType): CAPSRecords = CAPSRecords.empty()(caps)

      override def cache(): CAPSGraph = this

      override def persist(): CAPSGraph = this

      override def persist(storageLevel: StorageLevel): CAPSGraph = this

      override def unpersist(): CAPSGraph = this

      override def unpersist(blocking: Boolean): CAPSGraph = this
    }
    intercept[SchemaException] {
      buggyGraph.cypher("MATCH (n) RETURN n").getRecords
    }
    intercept[SchemaException] {
      buggyGraph.cypher("MATCH ()-[r]->() RETURN r").getRecords
    }
  }

}
