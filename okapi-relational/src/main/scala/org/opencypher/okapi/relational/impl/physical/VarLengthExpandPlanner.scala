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
package org.opencypher.okapi.relational.impl.physical

import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.types.CTBoolean
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.physical.{PhysicalOperator, PhysicalOperatorProducer, PhysicalPlannerContext, RuntimeContext}
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.impl.exception.RecordHeaderException
import org.opencypher.okapi.relational.impl.flat.FlatOperator
import org.opencypher.okapi.relational.impl.table.RecordHeader

trait ExpandDirection
case object Outbound extends ExpandDirection
case object Inbound extends ExpandDirection

trait VarLengthExpandPlanner[
O <: FlatRelationalTable[O],
K <: PhysicalOperator[O, A, P, I],
A <: RelationalCypherRecords[O],
P <: PropertyGraph,
I <: RuntimeContext[O, A, P]] {

  def source: Var

  def list: Var

  def edgeScan: Var

  def target: Var

  def lower: Int

  def upper: Int

  def sourceOp: FlatOperator

  def edgeScanOp: FlatOperator

  def targetOp: FlatOperator

  def isExpandInto: Boolean

  def planner: PhysicalPlanner[O, K, A, P, I]

  def plan: K

  implicit val context: PhysicalPlannerContext[O, K, A]
  val producer: PhysicalOperatorProducer[O, K, A, P, I] = planner.producer

  val physicalSourceOp: K = planner.process(sourceOp)
  val physicalEdgeScanOp: K = planner.process(edgeScanOp)
  val physicalTargetOp: K = planner.process(targetOp)

  val targetHeader: RecordHeader = {
    val aliasedEdgeScanCypherType = if (lower == 0) edgeScan.cypherType.nullable else edgeScan.cypherType
    val aliasedEdgeScan = ListSegment(1, list)(aliasedEdgeScanCypherType)
    val aliasedEdgeScanHeader = physicalEdgeScanOp.header.withAlias(edgeScan as aliasedEdgeScan).select(aliasedEdgeScan)

    val startHeader = physicalSourceOp.header join aliasedEdgeScanHeader


    def expand(i: Int, prev: RecordHeader): RecordHeader = {
      val edgeCypherType = if (i > lower) edgeScan.cypherType.nullable else edgeScan.cypherType
      val nextEdge = ListSegment(i, list)(edgeCypherType)

      val aliasedCacheHeader = physicalEdgeScanOp.header
        .withAlias(edgeScan as nextEdge)
        .select(nextEdge)

      prev join aliasedCacheHeader
    }

    val expandHeader = (2 to upper).foldLeft(startHeader) {
      case (acc, i) => expand(i, acc)
    }

    if (isExpandInto) expandHeader else expandHeader join physicalTargetOp.header
  }

  protected val startEdgeScan: Var = ListSegment(1, list)(edgeScan.cypherType)

  /**
    * Performs the initial expand from the start node
    *
    * @param dir expand direction
    */
  protected def init(dir: ExpandDirection): K = {
    val startEdgeScanOpAlias: K = producer.planAlias(
      physicalEdgeScanOp,
      edgeScan as startEdgeScan
    )
    val startEdgeScanOp: K = producer.planSelect(
      startEdgeScanOpAlias,
      startEdgeScanOpAlias.header.expressions.toList
    )

    // Execute the first expand
    val edgeJoinExpr = dir match {
      case Outbound => startEdgeScanOp.header.startNodeFor(startEdgeScan)
      case Inbound => startEdgeScanOp.header.endNodeFor(startEdgeScan)
    }

    val startOp = producer.planJoin(
      physicalSourceOp, startEdgeScanOp,
      Seq(source -> edgeJoinExpr)
    )
    producer.planFilter(startOp, isomorphismFilter(startEdgeScan, physicalSourceOp.header.relationshipEntities))
  }

  /**
    * Performs the ith expand.
    *
    * @param i              number of the iteration
    * @param iterationTable result of the i-1th iteration
    * @param directions     expansion directions
    * @param edgeVars       edges already traversed
    */
  def expand(i: Int, iterationTable: K, directions: (ExpandDirection, ExpandDirection), edgeVars: Seq[Var]): (K, Var) = {
    val nextEdgeCT = if (i > lower) edgeScan.cypherType.nullable else edgeScan.cypherType
    val nextEdge = ListSegment(i, list)(nextEdgeCT)

    val edgeScanOpWithAlias = producer.planAliases(physicalEdgeScanOp, Seq(edgeScan as nextEdge))

    val aliasedCacheHeader = edgeScanOpWithAlias.header.select( nextEdge)
    val selectExprs = aliasedCacheHeader.expressionsFor(nextEdge)
    val aliasedEdgeScanOp = producer.planSelect(edgeScanOpWithAlias, selectExprs.toList)

    val joinExpr = directions match {
      case (Outbound,Outbound) => iterationTable.header.endNodeFor(edgeVars.last) -> aliasedCacheHeader.startNodeFor(nextEdge)
      case (Outbound,Inbound) => iterationTable.header.endNodeFor(edgeVars.last) -> aliasedCacheHeader.endNodeFor(nextEdge)
      case (Inbound, Outbound) => iterationTable.header.startNodeFor(edgeVars.last) -> aliasedCacheHeader.endNodeFor(nextEdge)
      case (Inbound, Inbound) => iterationTable.header.startNodeFor(edgeVars.last) -> aliasedCacheHeader.startNodeFor(nextEdge)
    }

    val expandedOp = producer.planJoin(iterationTable, aliasedEdgeScanOp, Seq(joinExpr)
    )

    producer.planFilter(expandedOp, isomorphismFilter(nextEdge, edgeVars.toSet)) -> nextEdge
  }

  /**
    * Finalize the expansions
    *   1. adds paths of length zero if needed
    *   2. fills empty columns with null values
    *   3. unions paths of different lengths
    *
    * @param paths valid paths
    */
  protected def finalize(paths: Seq[K]): K = {
    // check whether to include paths of length 0
    val unalignedOps: Seq[K] = if (lower == 0) {
      val zeroLengthExpand: K = copyEntity(source, target, targetHeader, physicalSourceOp)
      if (upper == 0) Seq(zeroLengthExpand) else paths :+ zeroLengthExpand
    } else paths

    // fill shorter paths with nulls
    val alignedOps = unalignedOps.map { exp =>
      val nullExpressions = targetHeader.expressions -- exp.header.expressions
      nullExpressions.foldLeft(exp) {
        case (acc, expr) =>

          // TODO: this is a planning performance killer, we need to squash these steps into a single table operation
          val lit = NullLit(expr.cypherType)

          val withExpr = producer.planAddInto(acc, lit, expr)

          val withoutLit = producer.planDrop(withExpr, Set(lit))

          if (withoutLit.header.column(expr) == targetHeader.column(expr)) {
            withoutLit
          } else {
            val withRenamed = producer.planRenameColumns(withoutLit, Map(expr -> targetHeader.column(expr)))
            withRenamed
          }
      }
    }

    // union expands of different lengths
    alignedOps.reduce(producer.planTabularUnionAll)
  }

  /**
    * Creates the isomorphism filter for the given edge list
    *
    * @param rel        new edge
    * @param candidates candidate edges
    */
  protected def isomorphismFilter(rel: Var, candidates: Set[Var]): Ands = Ands(
    candidates.map(e => Not(Equals(e, rel)(CTBoolean))(CTBoolean)).toList
  )

  /**
    * Copies the content of a variable into another variable
    *
    * @param from       source variable
    * @param to         target variable
    * @param targetHeader     target header
    * @param physicalOp base operation
    */
  protected def copyEntity(
    from: Var,
    to: Var,
    targetHeader: RecordHeader,
    physicalOp: K
  ): K = {
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

    (childMapping ++ missingMapping).foldLeft(physicalOp) {
      case (acc, (f, t)) => producer.planAddInto(acc, f, t)
    }
  }

  /**
    * Joins a given path with it's target node
    *
    * @param path the path
    * @param edge the paths last edge
    * @param dir  expand direction
    */
  protected def addTargetOps(path: K, edge: Var, dir: ExpandDirection): K = {
    val expr = dir match {
      case Outbound => path.header.endNodeFor(edge)
      case Inbound => path.header.startNodeFor(edge)
    }

    if (isExpandInto) {
      val filterExpr = Equals(target, expr)(CTBoolean)
      producer.planFilter(path, filterExpr)
    } else {
      producer.planJoin(path, physicalTargetOp, Seq(expr -> target))
    }
  }
}

// TODO: use object instead
class DirectedVarLengthExpandPlanner[
O <: FlatRelationalTable[O],
K <: PhysicalOperator[O, A, P, I],
A <: RelationalCypherRecords[O],
P <: PropertyGraph,
I <: RuntimeContext[O, A, P]](
  override val source: Var,
  override val list: Var,
  override val edgeScan: Var,
  override val target: Var,
  override val lower: Int,
  override val upper: Int,
  override val sourceOp: FlatOperator,
  override val edgeScanOp: FlatOperator,
  override val targetOp: FlatOperator,
  override val isExpandInto: Boolean
)(
  override val planner: PhysicalPlanner[O, K, A, P, I],
  override implicit val context: PhysicalPlannerContext[O, K, A]
) extends VarLengthExpandPlanner[O, K, A, P, I] {

  override def plan: K = {
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
class UndirectedVarLengthExpandPlanner[
O <: FlatRelationalTable[O],
K <: PhysicalOperator[O, A, P, I],
A <: RelationalCypherRecords[O],
P <: PropertyGraph,
I <: RuntimeContext[O, A, P]](
  override val source: Var,
  override val list: Var,
  override val edgeScan: Var,
  override val target: Var,
  override val lower: Int,
  override val upper: Int,
  override val sourceOp: FlatOperator,
  override val edgeScanOp: FlatOperator,
  override val targetOp: FlatOperator,
  override val isExpandInto: Boolean
)(
  override val planner: PhysicalPlanner[O, K, A, P, I],
  override implicit val context: PhysicalPlannerContext[O, K, A]
) extends VarLengthExpandPlanner[O, K, A, P, I] {

  override def plan: K = {

    val outStartOp = init(Outbound)
    val inStartOp = init(Inbound)

    // Iteratively expand beginning from startOp with cacheOp
    val expandOps = (2 to upper).foldLeft(Seq((outStartOp -> inStartOp) -> Seq(startEdgeScan))) {
      case (acc, i) =>
        val ((last, lastRevered), edgeVars) = acc.last

        val (outOut, nextEdge) = expand(i, last, Outbound -> Outbound, edgeVars)
        val (outIn, _) = expand(i, last, Outbound -> Inbound, edgeVars)
        val (inOut, _) = expand(i, lastRevered, Inbound -> Outbound, edgeVars)
        val (inIn, _) = expand(i, lastRevered, Inbound ->Inbound, edgeVars)
        val nextOps = producer.planTabularUnionAll(outOut, inOut) -> producer.planTabularUnionAll(outIn, inIn)

        acc :+ nextOps -> (edgeVars :+ nextEdge)
    }.filter(_._2.size >= lower)


    // Join target nodes on expand ops
    val withTargetOps = expandOps.map {
      case ((out, in), edges) =>
        producer.planTabularUnionAll(
          addTargetOps(out, edges.last, Outbound),
          addTargetOps(in, edges.last, Inbound)
        )
    }

    finalize(withTargetOps)
  }
}
