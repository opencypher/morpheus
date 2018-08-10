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

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTString, CypherType}
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.operators.{RelationalOperator, Start}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.testing.CAPSTestSuite

class RecordHeaderMismatch extends CAPSTestSuite {

  it("throws a schema exception when the physical record header does not match the one computed based on the schema") {
    val buggyGraph = new RelationalCypherGraph[DataFrameTable] {

      override type Session = CAPSSession

      override type Records = CAPSRecords

      override def schema: CAPSSchema = Schema.empty
        .withNodePropertyKeys("A")("name" -> CTString)
        .withRelationshipPropertyKeys("R")("name" -> CTString)
        .asCaps

      override implicit def session: CAPSSession = caps

      override def tags: Set[Int] = Set(0)

      override def cache(): RelationalCypherGraph[DataFrameTable] = this

      override def tables: Seq[DataFrameTable] = Seq.empty

      // Always return empty records, which does not match what the schema promises
      def scanOperator(
        entityType: CypherType,
        exactLabelMatch: Boolean
      ): RelationalOperator[DataFrameTable] = {
        Start(caps.records.empty())
      }
    }

    intercept[SchemaException] {
      buggyGraph.cypher("MATCH (n) RETURN n").records
    }
    intercept[SchemaException] {
      buggyGraph.cypher("MATCH ()-[r]->() RETURN r").records
    }
  }

}
