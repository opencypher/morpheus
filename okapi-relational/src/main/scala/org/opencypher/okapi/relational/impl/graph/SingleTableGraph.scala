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
import org.opencypher.okapi.api.types.{CTBoolean, CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, RelType}
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}
import org.opencypher.okapi.relational.impl.operators._
import org.opencypher.okapi.relational.impl.planning.RelationalPlanner._

/**
  * A single table graph represents the result of CONSTRUCT clause. It contains all entities from the outer scope that
  * the clause constructs. The initial schema of that graph is the union of all graph schemata the CONSTRUCT clause refers
  * to, including their corresponding graph tags. Note, that the initial schema does not include the graph tag used for
  * the constructed entities.
  */
class SingleTableGraph[T <: Table[T]](
  val drivingTableOp: RelationalOperator[T],
  override val schema: Schema,
  override val tags: Set[Int]
)(implicit context: RelationalRuntimeContext[T]) extends RelationalCypherGraph[T] {

  override implicit val session: RelationalCypherSession[T] = context.session

  override type Session = RelationalCypherSession[T]

  override type Records = RelationalCypherRecords[T]

  private val header = drivingTableOp.header

  def show(): Unit = drivingTableOp.show()

  override def tables: Seq[T] = Seq(drivingTableOp.table)

  override def scanOperator(
    entityType: CypherType,
    exactLabelMatch: Boolean
  ): RelationalOperator[T] = {
    val targetEntity = Var("")(entityType)
    val targetEntityHeader = schema.headerForEntity(Var("")(entityType), exactLabelMatch)

    val extractionVars: Set[Var] = header.entitiesForType(entityType, exactLabelMatch)
    val extractedScans = extractionVars.map { extractionVar =>

      val labelOrTypePredicate = entityType match {
        case CTNode(labels, _) =>
          val labelExprs: Set[Expr] = labels.map(label => HasLabel(extractionVar, Label(label))(CTBoolean))
          val physicalExprs = labelExprs intersect header.expressionsFor(extractionVar)
          Ands(physicalExprs.map(expr => Equals(expr, TrueLit)(CTBoolean)))

        case CTRelationship(relTypes, _) =>
          val relTypeExprs: Set[Expr] = relTypes.map(relType => HasType(extractionVar, RelType(relType))(CTBoolean))
          val physicalExprs = relTypeExprs intersect header.expressionsFor(extractionVar)
          Ors(physicalExprs.map(expr => Equals(expr, TrueLit)(CTBoolean)))

        case other => throw IllegalArgumentException("CTNode or CTRelationship", other)
      }

      val selected = Select(drivingTableOp, List(extractionVar))
      val idExprs = header.idExpressions(extractionVar).toSeq

      val validEntityPredicate = Ands(idExprs.map(idExpr => IsNotNull(idExpr)(CTBoolean)) :+ labelOrTypePredicate: _*)
      val filtered = Filter(selected, validEntityPredicate)
      Distinct(filtered, Set(extractionVar))
    }

    val alignedScans = extractedScans.map(_.alignWith(targetEntity, targetEntityHeader)).toList

    alignedScans match {
      case Nil => Start(session.records.empty(targetEntityHeader))
      case singleOp :: Nil => singleOp
      case multipleOps => multipleOps.reduce(TabularUnionAll(_, _))
    }
  }

}
