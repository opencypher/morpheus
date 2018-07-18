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
package org.opencypher.spark.impl

import org.opencypher.okapi.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable

object CAPSConverters {

  implicit class RichSession(val session: CypherSession) extends AnyVal {
    def asCaps: CAPSSession = session match {
      case caps: CAPSSession => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS sessions, got $session")
    }
  }

  implicit class RichPropertyGraph(val graph: PropertyGraph) extends AnyVal {
    def asCaps: RelationalCypherGraph[DataFrameTable] = graph.asInstanceOf[RelationalCypherGraph[_]] match {
      // We know what we did... the alternatives were worse.
      case caps: RelationalCypherGraph[_] if caps.tables.forall(_.isInstanceOf[DataFrameTable]) =>
        caps.asInstanceOf[RelationalCypherGraph[DataFrameTable]]
      case _ => throw UnsupportedOperationException(s"can only handle CAPS graphs, got $graph")
    }
  }

  implicit class RichCypherRecords(val records: CypherRecords) extends AnyVal {
    def asCaps(implicit caps: CAPSSession): CAPSRecords = records match {
      // AGAIN! We still know what we do... the alternatives were worse.
      case relational: RelationalCypherRecords[_] if relational.table.isInstanceOf[DataFrameTable] =>
        caps.records.from(relational.header, relational.table.asInstanceOf[DataFrameTable])
      case caps: CAPSRecords => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS records, got $records")
    }
  }

}
