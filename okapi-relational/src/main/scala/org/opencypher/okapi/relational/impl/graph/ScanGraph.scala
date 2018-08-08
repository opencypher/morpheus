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
package org.opencypher.okapi.relational.impl.graph

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.io.{EntityTable, NodeTable, RelationshipTable}
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}
import org.opencypher.okapi.relational.impl.operators._
import org.opencypher.okapi.relational.impl.planning.RelationalPlanner._

class ScanGraph[T <: Table[T]](val scans: Seq[EntityTable[T]], val schema: Schema, val tags: Set[Int])
  (implicit val session: RelationalCypherSession[T])
  extends RelationalCypherGraph[T] {

  override type Records = RelationalCypherRecords[T]

  override type Session = RelationalCypherSession[T]

  private lazy val nodeTables = scans.collect { case it: NodeTable[T] => it }

  private lazy val relTables = scans.collect { case it: RelationshipTable[T] => it }

  // TODO: ScanGraph should be an operator that gets a set of tables as input
  private implicit def runtimeContext: RelationalRuntimeContext[T] = session.basicRuntimeContext()

  override def tables: Seq[T] = scans.map(_.table)

  // TODO: Express `exactLabelMatch` with type
  private[opencypher] override def scanOperator(
    entityType: CypherType,
    exactLabelMatch: Boolean
  ): RelationalOperator[T] = {
    val targetEntity = Var("")(entityType)
    val selectedScans = scansForType(entityType, exactLabelMatch)
    val targetEntityHeader = schema.headerForEntity(targetEntity, exactLabelMatch)
    val alignedEntityTableOps = selectedScans.map { scan =>
      scan.alignWith(targetEntity, targetEntityHeader)
    }

    alignedEntityTableOps.toList match {
      case Nil => Start(session.records.empty(targetEntityHeader))
      case singleOp :: Nil => singleOp
      case multipleOps => multipleOps.reduce(TabularUnionAll(_, _))
    }
  }

  // TODO: Express `exactLabelMatch` with type
  private def scansForType(ct: CypherType, exactLabelMatch: Boolean): Seq[RelationalOperator[T]] = {
    ct match {
      case _: CTNode =>
        val scans = if (exactLabelMatch) {
          nodeTables.filter(_.entityType == ct)
        } else {
          nodeTables.filter(_.entityType.subTypeOf(ct).isTrue)
        }
        scans.map(scanRecords => Start(scanRecords))
      case r: CTRelationship =>
        relTables
          .filter(relTable => relTable.entityType.couldBeSameTypeAs(ct))
          .map(scanRecords => Start(scanRecords).filterRelTypes(r))

      case other => throw IllegalArgumentException(s"Scan on $other")
    }
  }



  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean = false): Records = {
    val scan = scanOperator(nodeCypherType, exactLabelMatch)
    val namedScan = scan.assignScanName(name)
    session.records.from(namedScan.header, namedScan.table)
  }

  override def relationships(name: String, relCypherType: CTRelationship): Records = {
    val scan = scanOperator(relCypherType, exactLabelMatch = false)
    val namedScan = scan.assignScanName(name)
    session.records.from(namedScan.header, namedScan.table)
  }

  override def toString = s"ScanGraph(${
    scans.map(_.entityType).mkString(", ")
  })"

}
