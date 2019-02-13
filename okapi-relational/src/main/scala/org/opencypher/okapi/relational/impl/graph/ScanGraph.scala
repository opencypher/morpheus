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
package org.opencypher.okapi.relational.impl.graph

import org.opencypher.okapi.api.graph.{NodePattern, RelationshipPattern}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}
import org.opencypher.okapi.relational.impl.operators._
import org.opencypher.okapi.relational.impl.planning.RelationalPlanner._

import scala.reflect.runtime.universe.TypeTag

class ScanGraph[T <: Table[T] : TypeTag](val scans: Seq[EntityTable[T]], val schema: Schema)
  (implicit val session: RelationalCypherSession[T])
  extends RelationalCypherGraph[T] {

  override type Records = RelationalCypherRecords[T]

  override type Session = RelationalCypherSession[T]

  // TODO: ScanGraph should be an operator that gets a set of tables as input
  private implicit def runtimeContext: RelationalRuntimeContext[T] = session.basicRuntimeContext()

  override def tables: Seq[T] = scans.map(_.table)

  // TODO: Express `exactLabelMatch` with type
  override def scanOperator(
    entityType: CypherType,
    exactLabelMatch: Boolean
  ): RelationalOperator[T] = {
    val targetEntity = Var.unnamed(entityType)
    val selectedScans = scansForType(entityType, exactLabelMatch)
    val targetEntityHeader = schema.headerForEntity(targetEntity, exactLabelMatch)
    val alignedEntityTableOps = selectedScans.map { scan =>
      val inputEntity = scan.singleEntity
      scan.alignWith(inputEntity, targetEntity, targetEntityHeader)
    }

    alignedEntityTableOps.toList match {
      case Nil => Start.fromEmptyGraph(session.records.empty(targetEntityHeader))
      case singleOp :: Nil => singleOp
      case multipleOps => multipleOps.reduce(TabularUnionAll(_, _))
    }
  }

  // TODO: Express `exactLabelMatch` with type
  private def scansForType(ct: CypherType, exactLabelMatch: Boolean): Seq[RelationalOperator[T]] = {
    val qgn = ct.graph.getOrElse(session.emptyGraphQgn)
    ct match {
      case nodeType@CTNode(labels, _) =>
        val nodePatterns = scans.collect {
          case e: EntityTable[_] if e.mapping.pattern.isInstanceOf[NodePattern] => e -> e.mapping.pattern.asInstanceOf[NodePattern]
        }
        val selectedScans = if (exactLabelMatch) {
          nodePatterns.filter(_._2.nodeEntity.typ == ct)
        } else {
          nodePatterns.filter(_._2.nodeEntity.typ.subTypeOf(ct).isTrue)
        }
        val startOpsForImpliedLabels = selectedScans.map(scanRecords => Start(qgn, scanRecords._1))

        val startOpsForOptionalLabels = if (labels.isEmpty) {
          Seq.empty
        } else {
          nodePatterns
            .filter(_._2.nodeEntity.typ == CTNode)
            .filter {
              case(table, pattern) => labels subsetOf table.mapping.optionalTypes(pattern.nodeEntity).keySet
            }
            .map { table => Start(qgn, table._1).filterNodeLabels(nodeType, exactLabelMatch) }
        }

        startOpsForImpliedLabels ++ startOpsForOptionalLabels

      case r: CTRelationship =>
        val relPatterns = scans.collect {
          case e: EntityTable[_] if e.mapping.pattern.isInstanceOf[RelationshipPattern] => e -> e.mapping.pattern.asInstanceOf[RelationshipPattern]
        }
        relPatterns
          .filter { case (table, pattern) => pattern.relEntity.typ.couldBeSameTypeAs(ct) }
          .map(scanRecords => Start(qgn, scanRecords._1).filterRelTypes(r))

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
    scans.map(_.mapping.pattern).mkString(", ")
  })"

}
