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
package org.opencypher.spark.test.support.creation.caps

import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.test.support.creation.propertygraph.TestPropertyGraph
import org.opencypher.okapi.relational.impl.table.ColumnName
import org.opencypher.okapi.relational.impl.table.RecordHeader._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.table.CAPSRecordHeader._
import org.opencypher.spark.impl.{CAPSGraph, CAPSPatternGraph, CAPSRecords}

object CAPSPatternGraphFactory extends CAPSTestGraphFactory {

  override def apply(propertyGraph: TestPropertyGraph)(implicit caps: CAPSSession): CAPSGraph = {
    val scanGraph = CAPSScanGraphFactory(propertyGraph)
    val nodes = scanGraph.nodes("n")
    val rels = scanGraph.relationships("r")

    val lhs = nodes.data.col(ColumnName.of(Var("n")(CTNode)))
    val rhs = rels.data.col(rels.header.sourceNodeSlot(Var("r")(CTRelationship)).columnName)

    val baseTableData = nodes.data.join(rels.data, lhs === rhs, "left_outer")

    val baseTable = CAPSRecords.verifyAndCreate(nodes.header ++ rels.header, baseTableData)

    new CAPSPatternGraph(baseTable, scanGraph.schema, Set(0))
  }

  override def name: String = "CAPSPatternGraphFactory"
}
