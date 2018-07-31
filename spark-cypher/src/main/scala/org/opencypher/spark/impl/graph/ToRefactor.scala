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
package org.opencypher.spark.impl.graph

import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr.{Expr, Property, Var}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable

object ToRefactor {

  // TODO: move this to okapi-relational and plan as operators
  def nodesWithExactLabels(graph: RelationalCypherGraph[DataFrameTable], name: String, labels: Set[String])
    (implicit caps: CAPSSession): CAPSRecords = {
    val nodeType = CTNode(labels)
    val nodeVar = Var(name)(nodeType)
    val records = graph.nodes(name, nodeType)

    val header = records.header

    val labelExprs = header.labelsFor(nodeVar)

    val propertyExprs = graph.schema.nodeKeys(labels).flatMap {
      case (key, cypherType) => Property(nodeVar, PropertyKey(key))(cypherType)
    }.toSet
    val headerPropertyExprs = header.propertiesFor(nodeVar).filter(propertyExprs.contains)

    val keepExprs: Seq[Expr] = Seq(nodeVar) ++ labelExprs ++ headerPropertyExprs

    val keepColumns = keepExprs.map(header.column)

    // we only keep rows where all "other" labels are false
    val predicate = labelExprs
      .filterNot(l => labels.contains(l.label.name))
      .map(header.column)
      .map(records.table.df.col(_) === false)
      .reduceOption(_ && _)

    // filter rows and select only necessary columns
    val updatedData = predicate match {

      case Some(filter) =>
        records.table.df
          .filter(filter)
          .select(keepColumns.head, keepColumns.tail: _*)

      case None =>
        records.table.df.select(keepColumns.head, keepColumns.tail: _*)
    }

    val updatedHeader = RecordHeader.from(keepExprs)

    caps.records.from(updatedHeader, updatedData)
  }

}
