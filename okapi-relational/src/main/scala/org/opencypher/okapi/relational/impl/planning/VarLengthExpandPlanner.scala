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
package org.opencypher.okapi.relational.impl.planning

import org.opencypher.okapi.api.types.CTBoolean
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.exception.RecordHeaderException
import org.opencypher.okapi.relational.impl.operators.RelationalOperator
import org.opencypher.okapi.relational.impl.planning.RelationalPlanner.{process, _}
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.relational.impl.{operators => relational}

import scala.reflect.runtime.universe.TypeTag

sealed trait ExpandDirection
case object Outbound extends ExpandDirection
case object Inbound extends ExpandDirection

abstract class VarLengthExpandPlanner[T <: Table[T] : TypeTag] {

  def source: Var

  def list: Var

  def edgeScan: Var

  def target: Var

  def lower: Int

  def upper: Int

  def sourceOp: LogicalOperator

  def relEdgeScanOp: RelationalOperator[T]

  def targetOp: LogicalOperator

  def isExpandInto: Boolean

  def plan: RelationalOperator[T]

  implicit val context: RelationalRuntimeContext[T]

  val physicalSourceOp: RelationalOperator[T] = process(sourceOp)
  val physicalEdgeScanOp: RelationalOperator[T] = relEdgeScanOp
  val physicalTargetOp: RelationalOperator[T] = process(targetOp)

  protected val startEdgeScan: Var = ListSegment(1, list)(edgeScan.cypherType)

  /**
    * Performs the initial expand from the start node
    *
    * @param dir expand direction
    */
  protected def init(dir: ExpandDirection): RelationalOperator[T] = {
    val startEdgeScanOp: RelationalOperator[T] = physicalEdgeScanOp
      .alias(edgeScan as startEdgeScan)
      .select(startEdgeScan)

    // Execute the first expand
    val edgeJoinExpr = dir match {
      case Outbound => startEdgeScanOp.header.startNodeFor(startEdgeScan)
      case Inbound => startEdgeScanOp.header.endNodeFor(startEdgeScan)
    }

    physicalSourceOp.join(startEdgeScanOp,
      Seq(source -> edgeJoinExpr),
      InnerJoin
    ).filter(isomorphismFilter(startEdgeScan, physicalSourceOp.header.relationshipEntities))
  }

  /**
    * Performs the ith expand.
    *
    * @param i              number of the iteration
    * @param iterationTable result of the i-1th iteration
    * @param directions     expansion directions
    * @param edgeVars       edges already traversed
    */
  def expand(
    i: Int,
    iterationTable: RelationalOperator[T],
    directions: (ExpandDirection, ExpandDirection),
    edgeVars: Seq[Var]
  ): (RelationalOperator[T], Var) = {
    val nextEdgeCT = if (i > lower) edgeScan.cypherType.nullable else edgeScan.cypherType
    val nextEdge = ListSegment(i, list)(nextEdgeCT)

    val aliasedEdgeScanOp = physicalEdgeScanOp.
      alias(edgeScan as nextEdge)
      .select(nextEdge)

    val iterationTableHeader = iterationTable.header
    val nextEdgeScanHeader = aliasedEdgeScanOp.header

    val joinExpr = directions match {
      case (Outbound, Outbound) => iterationTableHeader.endNodeFor(edgeVars.last) -> nextEdgeScanHeader.startNodeFor(nextEdge)
      case (Outbound, Inbound) => iterationTableHeader.endNodeFor(edgeVars.last) -> nextEdgeScanHeader.endNodeFor(nextEdge)
      case (Inbound, Outbound) => iterationTableHeader.startNodeFor(edgeVars.last) -> nextEdgeScanHeader.endNodeFor(nextEdge)
      case (Inbound, Inbound) => iterationTableHeader.startNodeFor(edgeVars.last) -> nextEdgeScanHeader.startNodeFor(nextEdge)
    }

    val expandedOp = iterationTable
      .join(aliasedEdgeScanOp, Seq(joinExpr), InnerJoin)
      .filter(isomorphismFilter(nextEdge, edgeVars.toSet))

    expandedOp -> nextEdge
  }

  /**
    * Finalize the expansions
    *   1. adds paths of length zero if needed
    *   2. fills empty columns with null values
    *   3. unions paths of different lengths
    *
    * @param paths valid paths
    */
  protected def finalize(paths: Seq[RelationalOperator[T]]): RelationalOperator[T] = {
    val targetHeader = paths.maxBy(_.header.columns.size).header

    // check whether to include paths of length 0
    val unalignedOps: Seq[RelationalOperator[T]] = if (lower == 0) {
      val zeroLengthExpand: RelationalOperator[T] = copyEntity(source, target, targetHeader, physicalSourceOp)
      if (upper == 0) Seq(zeroLengthExpand) else paths :+ zeroLengthExpand
    } else paths

    // fill shorter paths with nulls
    val alignedOps = unalignedOps.map { expansion =>
      val nullExpressions = targetHeader.expressions -- expansion.header.expressions

      val expWithNullLits = expansion.addInto(nullExpressions.map(expr => NullLit(expr.cypherType.nullable) -> expr).toSeq: _*)
      val exprsToRename = nullExpressions.filterNot(expr =>
        expWithNullLits.header.column(expr) == targetHeader.column(expr)
      )
      val renameTuples = exprsToRename.map(expr => expr -> targetHeader.column(expr))
      expWithNullLits.renameColumns(renameTuples.toMap)
    }

    // union expands of different lengths
    alignedOps
      .map(op => op.alignColumnNames(targetHeader))
      .reduce((agg: RelationalOperator[T], next: RelationalOperator[T]) => relational.TabularUnionAll(agg, next))
  }

  /**
    * Creates the isomorphism filter for the given edge list
    *
    * @param rel        new edge
    * @param candidates candidate edges
    */
  protected def isomorphismFilter(rel: Var, candidates: Set[Var]): Expr =
    Ands(candidates.map(e => Not(Equals(e, rel)(CTBoolean))(CTBoolean)).toSeq: _*)

  /**
    * Copies the content of a variable into another variable
    *
    * @param from         source variable
    * @param to           target variable
    * @param targetHeader target header
    * @param physicalOp   base operation
    */
  protected def copyEntity(
    from: Var,
    to: Var,
    targetHeader: RecordHeader,
    physicalOp: RelationalOperator[T]
  ): RelationalOperator[T] = {
    // TODO: remove when https://github.com/opencypher/cypher-for-apache-spark/issues/513 is resolved
    val correctTarget = targetHeader.entityVars.find(_ == to).get

    val sourceChildren = targetHeader.expressionsFor(from)
    val targetChildren = targetHeader.expressionsFor(correctTarget)

    val childMapping: Set[(Expr, Expr)] = sourceChildren.map(expr => expr -> expr.withOwner(correctTarget))
    val missingMapping = (targetChildren -- childMapping.map(_._2) - correctTarget).map {
      case l: HasLabel => FalseLit -> l
      case p: Property => NullLit(p.cypherType) -> p
      case other => throw RecordHeaderException(s"$correctTarget can only own HasLabel and Property but found $other")
    }

    physicalOp.addInto((childMapping ++ missingMapping).toSeq: _*)
  }

  /**
    * Joins a given path with it's target node
    *
    * @param path the path
    * @param edge the paths last edge
    * @param dir  expand direction
    */
  protected def addTargetOps(path: RelationalOperator[T], edge: Var, dir: ExpandDirection): RelationalOperator[T] = {
    val expr = dir match {
      case Outbound => path.header.endNodeFor(edge)
      case Inbound => path.header.startNodeFor(edge)
    }

    if (isExpandInto) {
      path.filter(Equals(target, expr)(CTBoolean))
    } else {
      path.join(physicalTargetOp, Seq(expr -> target), InnerJoin)
    }
  }
}

// TODO: use object instead
class DirectedVarLengthExpandPlanner[T <: Table[T] : TypeTag](
  override val source: Var,
  override val list: Var,
  override val edgeScan: Var,
  override val target: Var,
  override val lower: Int,
  override val upper: Int,
  override val sourceOp: LogicalOperator,
  override val relEdgeScanOp: RelationalOperator[T],
  override val targetOp: LogicalOperator,
  override val isExpandInto: Boolean
)(override implicit val context: RelationalRuntimeContext[T]) extends VarLengthExpandPlanner[T] {

  override def plan: RelationalOperator[T] = {
    // Iteratively expand beginning from startOp with cacheOp
    val expandOps = (2 to upper).foldLeft(Seq(init(Outbound) -> Seq(startEdgeScan))) {
      case (acc, i) =>
        val (last, edgeVars) = acc.last
        val (next, nextEdge) = expand(i, last, Outbound -> Outbound, edgeVars)
        acc :+ (next -> (edgeVars :+ nextEdge))
    }.filter(_._2.size >= lower)

    // Join target nodes on expand ops
    val withTargetOps = expandOps.map { case (op, edges) => addTargetOps(op, edges.last, Outbound) }

    finalize(withTargetOps)
  }

}

// TODO: use object instead
class UndirectedVarLengthExpandPlanner[T <: Table[T] : TypeTag](
  override val source: Var,
  override val list: Var,
  override val edgeScan: Var,
  override val target: Var,
  override val lower: Int,
  override val upper: Int,
  override val sourceOp: LogicalOperator,
  override val relEdgeScanOp: RelationalOperator[T],
  override val targetOp: LogicalOperator,
  override val isExpandInto: Boolean
)(override implicit val context: RelationalRuntimeContext[T]) extends VarLengthExpandPlanner[T] {

  override def plan: RelationalOperator[T] = {

    val outStartOp = init(Outbound)
    val inStartOp = init(Inbound)

    // Iteratively expand beginning from startOp with cacheOp
    val expandOps = (2 to upper).foldLeft(Seq((outStartOp -> inStartOp) -> Seq(startEdgeScan))) {
      case (acc, i) =>
        val ((last, lastRevered), edgeVars) = acc.last

        val (outOut, nextEdge) = expand(i, last, Outbound -> Outbound, edgeVars)
        val (outIn, _) = expand(i, last, Outbound -> Inbound, edgeVars)
        val (inOut, _) = expand(i, lastRevered, Inbound -> Outbound, edgeVars)
        val (inIn, _) = expand(i, lastRevered, Inbound -> Inbound, edgeVars)
        val nextOps = relational.TabularUnionAll(outOut, inOut) -> relational.TabularUnionAll(outIn, inIn)

        acc :+ nextOps -> (edgeVars :+ nextEdge)
    }.filter(_._2.size >= lower)


    // Join target nodes on expand ops
    val withTargetOps = expandOps.map {
      case ((out, in), edges) =>
        relational.TabularUnionAll(
          addTargetOps(out, edges.last, Outbound),
          addTargetOps(in, edges.last, Inbound)
        )
    }

    finalize(withTargetOps)
  }

}
