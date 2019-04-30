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
package org.opencypher.morpheus.impl.physical

import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.impl.MorpheusRecords
import org.opencypher.morpheus.impl.table.SparkTable.DataFrameTable
import org.opencypher.morpheus.schema.MorpheusSchema
import org.opencypher.morpheus.schema.MorpheusSchema._
import org.opencypher.morpheus.testing.MorpheusTestSuite
import org.opencypher.okapi.api.graph.Pattern
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types.CTString
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.operators.{RelationalOperator, Start}

class RecordHeaderMismatch extends MorpheusTestSuite {

  it("throws a schema exception when the physical record header does not match the one computed based on the schema") {
    val buggyGraph: RelationalCypherGraph[DataFrameTable] {
      type Session = MorpheusSession

      type Records = MorpheusRecords
    } = new RelationalCypherGraph[DataFrameTable] {

      override type Session = MorpheusSession

      override type Records = MorpheusRecords

      override def schema: MorpheusSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys("A")("name" -> CTString)
        .withRelationshipPropertyKeys("R")("name" -> CTString)
        .asMorpheus

      override implicit def session: MorpheusSession = morpheus

      override def cache(): RelationalCypherGraph[DataFrameTable] = this

      override def tables: Seq[DataFrameTable] = Seq.empty

      // Always return empty records, which does not match what the schema promises
      def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean): RelationalOperator[DataFrameTable] = {
        Start.fromEmptyGraph(morpheus.records.empty())
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
