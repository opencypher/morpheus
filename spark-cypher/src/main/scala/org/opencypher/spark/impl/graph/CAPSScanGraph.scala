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

import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.impl.operators._
import org.opencypher.okapi.relational.impl.physical.RelationalPlanner._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable
import org.opencypher.spark.schema.CAPSSchema

class CAPSScanGraph(val scans: Seq[CAPSEntityTable], val schema: CAPSSchema, val tags: Set[Int])
  (implicit val session: CAPSSession)
  extends RelationalCypherGraph[DataFrameTable] {

  override type Records = CAPSRecords

  override type Session = CAPSSession

  private lazy val nodeTables = scans.collect { case it: CAPSNodeTable => it }

  private lazy val relTables = scans.collect { case it: CAPSRelationshipTable => it }

  // TODO: ScanGraph should be an operator that gets a set of tables as input
  private implicit def runtimeContext: RelationalRuntimeContext[DataFrameTable] = session.basicRuntimeContext()

  override def tables: Seq[DataFrameTable] = scans.map(_.table)

  // TODO: Express `exactLabelMatch` with type
  private[opencypher] override def scanOperator(
    entityType: CypherType,
    exactLabelMatch: Boolean
  ): RelationalOperator[DataFrameTable] = {
    val entity = Var("")(entityType)
    val selectedScans = scansForType(entityType, exactLabelMatch)
    val targetEntityHeader = schema.headerForEntity(entity, exactLabelMatch)
    val entityWithCorrectType = targetEntityHeader.entityVars.head
    val alignedEntityTableOps = selectedScans.map { scan =>
      scan.alignWith(entityWithCorrectType, targetEntityHeader)
    }

    alignedEntityTableOps.toList match {
      case Nil => Start(session.records.empty(targetEntityHeader))
      case singleOp :: Nil => singleOp
      case multipleOps => multipleOps.reduce(TabularUnionAll(_, _))
    }
  }

  // TODO: Express `exactLabelMatch` with type
  private def scansForType(ct: CypherType, exactLabelMatch: Boolean): Seq[RelationalOperator[DataFrameTable]] = {
    ct match {
      case _: CTNode =>
        val scans = if (exactLabelMatch) {
          nodeTables.filter(_.entityType == ct)
        } else {
          nodeTables.filter(_.entityType.subTypeOf(ct).isTrue)
        }
        scans.map(scan => Start(scan.records))
      case r: CTRelationship =>
        relTables
          .filter(relTable => relTable.entityType.couldBeSameTypeAs(ct))
          .map(scan => Start(scan.records).filterRelTypes(r))

      case other => throw IllegalArgumentException(s"Scan on $other")
    }
  }



  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean = false): CAPSRecords = {
    val scan = scanOperator(nodeCypherType, exactLabelMatch)
    val namedScan = scan.assignScanName(name)
    session.records.from(namedScan.header, namedScan.table)
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val scan = scanOperator(relCypherType, exactLabelMatch = false)
    val namedScan = scan.assignScanName(name)
    session.records.from(namedScan.header, namedScan.table)
  }

  override def toString = s"CAPSScanGraph(${
    scans.map(_.entityType).mkString(", ")
  })"

}
