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

import org.opencypher.okapi.api.graph.{Entity, NodePattern, Pattern, RelationshipPattern}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.impl.util.VarConverters._
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

  validate()

  override type Records = RelationalCypherRecords[T]

  override type Session = RelationalCypherSession[T]

  // TODO: ScanGraph should be an operator that gets a set of tables as input
  private implicit def runtimeContext: RelationalRuntimeContext[T] = session.basicRuntimeContext()

  override def tables: Seq[T] = scans.map(_.table)

  // TODO: Express `exactLabelMatch` with type
  override def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean): RelationalOperator[T] = {
    val selectedScans = scansForType(searchPattern, exactLabelMatch)

    val alignedEntityTableOps = selectedScans.map {
      case (scan, embedding) =>
        embedding.foldLeft(scan) {
          case (acc, (targetEntity, inputEntity)) =>
            val inputEntityExpressions = scan.header.expressionsFor(inputEntity.toVar)
            val targetHeader = acc.header -- inputEntityExpressions ++ schema.headerForEntity(targetEntity.toVar, exactLabelMatch)

            acc.alignWith(inputEntity.toVar, targetEntity.toVar, targetHeader)
        }
    }

    alignedEntityTableOps.toList match {
      case Nil =>
        val scanHeader = searchPattern
          .entities
          .map { e => schema.headerForEntity(e.toVar) }
          .reduce(_ ++ _)

        Start.fromEmptyGraph(session.records.unit(scanHeader))

      case singleOp :: Nil => singleOp

      case multipleOps => multipleOps.reduce(TabularUnionAll(_, _))
    }
  }

  // TODO: Express `exactLabelMatch` with type
  private def scansForType(
    searchPattern: Pattern,
    exactLabelMatch: Boolean
  ): Seq[(RelationalOperator[T], Map[Entity, Entity])] = {
    val qgn = searchPattern.entities.head.cypherType.graph.getOrElse(session.emptyGraphQgn)

    val selectedScans = scans.flatMap { scan =>
      val scanPattern = scan.mapping.pattern
      scanPattern
        .findMapping(searchPattern, exactLabelMatch)
        .map(embedding => scan -> embedding)
    }

    selectedScans.map {
      case (scan, embedding) => Start(qgn, scan) -> embedding
    }
  }

  override lazy val patterns: Set[Pattern] = scans.map { scan =>
    scan.mapping.pattern
  }.toSet

  override def toString = s"ScanGraph(${
    scans.map(_.mapping.pattern).mkString(", ")
  })"

  def validate(): Unit = {
    schema.labelCombinations.combos.foreach { combo =>
      val hasScan = patterns.exists {
        case NodePattern(nodeType) => nodeType.labels == combo
        case _ => false
      }

      if(!hasScan) {
        throw IllegalArgumentException(
          s"a scan with NodePattern for label combination $combo",
          patterns
        )
      }
    }

    schema.relationshipTypes.foreach { relType =>
      val hasScan = patterns.exists {
        case RelationshipPattern(relTypes) => relTypes.types.head == relType
        case _ => false
      }

      if(!hasScan) {
        throw IllegalArgumentException(
          s"a scan with a RelationshipPattern for relationship type $relType",
          patterns
        )
      }
    }
  }
}
