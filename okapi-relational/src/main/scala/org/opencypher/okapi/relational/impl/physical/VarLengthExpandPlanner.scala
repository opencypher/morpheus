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
import org.opencypher.okapi.relational.api.io.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.api.physical.{PhysicalOperator, PhysicalOperatorProducer, PhysicalPlannerContext, RuntimeContext}
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

  def edgeScan: Var

  def innerNode: Var

  def target: Var

  def lower: Int

  def upper: Int

  def sourceOp: FlatOperator

  def edgeScanOp: FlatOperator

  def innerNodeOp: FlatOperator

  def targetOp: FlatOperator

  def header: RecordHeader

  def isExpandInto: Boolean

  def planner: PhysicalPlanner[O, K, A, P, I]

  def plan: K

  implicit val context: PhysicalPlannerContext[O, K, A]
  val producer: PhysicalOperatorProducer[O, K, A, P, I] = planner.producer

  val physicalSourceOp: K = planner.process(sourceOp)
  val physicalEdgeScanOp: K = planner.process(edgeScanOp)
  val physicalInnerNodeOp: K = planner.process(innerNodeOp)
  val physicalTargetOp: K = planner.process(targetOp)

  val startEdgeScan: Var = header.entityVars.find(_.name == s"${edgeScan.name}_1").get
  val startEdgeScanOp: K = producer.planAlias(
    physicalEdgeScanOp,
    edgeScan as startEdgeScan,
    edgeScanOp.header.withAlias(edgeScan as startEdgeScan).select(startEdgeScan)
  )

  /**
    * Performs the initial expand from the start node
    *
    * @param dir expand direction
    */
  protected def init(dir: ExpandDirection): K = {
    // Execute the first expand
    val edgeJoinExpr = dir match {
      case Outbound => startEdgeScanOp.header.startNodeFor(startEdgeScan)
      case Inbound => startEdgeScanOp.header.endNodeFor(startEdgeScan)
    }

    val startOp = producer.planJoin(
      physicalSourceOp, startEdgeScanOp,
      Seq(source -> edgeJoinExpr),
      sourceOp.header join startEdgeScanOp.header
    )
    producer.planFilter(startOp, isomorphismFilter(startEdgeScan, sourceOp.header.relationshipVars), startOp.header)
  }

  /**
    * Performs the ith expand.
    *
    * @param i              number of the iteration
    * @param iterationTable result of the i-1th iteration
    * @param expandCacheOp  expand cache used for this expand
    * @param dir            expansion direction
    * @param edgeVars       edges already travesed
    */
  def expand(i: Int, iterationTable: K, expandCacheOp: K, dir: ExpandDirection, edgeVars: Seq[Var]): (K, Var) = {
    val nextNode = header.entityVars.find(_.name == s"${innerNode.name}_${i - 1}").get
    val nextEdge = header.entityVars.find(_.name == s"${edgeScan.name}_$i").get

    val aliasedCacheHeader = expandCacheOp.header
      .withAlias(edgeScan as nextEdge, innerNode as nextNode)

    val aliasedCacheOp = producer.planAliases(
      expandCacheOp, Seq(edgeScan as nextEdge, innerNode as nextNode),
      aliasedCacheHeader
    )

    // We just want to select id columns, select always selects also the children. we need to drop them first
    // TODO: this is a planning performance killer, we need to squash these steps into a single table operation
    val idExprs = Set(
      nextEdge,
      aliasedCacheHeader.startNodeFor(nextEdge),
      aliasedCacheHeader.endNodeFor(nextEdge),
      nextNode)

    val dropExprs = idExprs.flatMap(aliasedCacheHeader.expressionsFor) -- idExprs
    val dropHeader = aliasedCacheHeader -- dropExprs
    val withChildExprsDropped = producer.planDrop(aliasedCacheOp, dropExprs, dropHeader)

    val aliasSelectHeader = dropHeader.select(idExprs)
    val selectedCacheOp = producer.planSelect(withChildExprsDropped, idExprs.toList, aliasSelectHeader)

    val leftJoinExpr = dir match {
      case Outbound => iterationTable.header.endNodeFor(edgeVars.last)
      case Inbound => iterationTable.header.startNodeFor(edgeVars.last)
    }

    val expandedOp = producer.planJoin(
      iterationTable,
      selectedCacheOp,
      Seq(leftJoinExpr -> nextNode),
      iterationTable.header join aliasSelectHeader
    )

    producer.planFilter(expandedOp, isomorphismFilter(nextEdge, edgeVars.toSet), expandedOp.header) -> nextEdge
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
    val unalignedOps = if (lower == 0) {
      val zeroLengthExpand: K = copyVar(source, target, header, physicalSourceOp)
      if (upper == 0) Seq(zeroLengthExpand) else paths :+ zeroLengthExpand
    } else paths

    // fill shorter paths with nulls
    val alignedOps = unalignedOps.map { exp =>
      val nullExpressions = header.expressions -- exp.header.expressions
      nullExpressions.foldLeft(exp) {
        case (acc, expr) =>

          // TODO: this is a planning performance killer, we need to squash these steps into a single table operation
          val lit = NullLit(expr.cypherType)

          val withExprHeader = acc.header.withExpr(expr)
          val withExpr = producer.planCopyColumn(acc, lit, expr, withExprHeader)

          val withoutLitHeader = withExprHeader -- Set(lit)
          val withoutLit = producer.planDrop(withExpr, Set(lit), withoutLitHeader)

          if (withoutLitHeader.column(expr) == header.column(expr)) {
            withoutLit
          } else {
            val withRenamedHeader = (withoutLitHeader -- Set(expr)).addExprToColumn(expr, header.column(expr))
            val withRenamed = producer.planRenameColumns(withoutLit, Map(expr -> header.column(expr)), withRenamedHeader)
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
    * @param header     target header
    * @param physicalOp base operation
    */
  protected def copyVar(
    from: Var,
    to: Var,
    header: RecordHeader,
    physicalOp: K
  ): K = {
    // TODO: remove when https://github.com/opencypher/cypher-for-apache-spark/issues/513 is resolved
    val correctTarget = header.entityVars.find(_ == to).get

    val sourceChildren = header.expressionsFor(from)
    val targetChildren = header.expressionsFor(correctTarget)

    val childMapping: Set[(Expr, Expr)] = sourceChildren.map(expr => expr -> expr.withOwner(correctTarget))
    val missingMapping = (targetChildren -- childMapping.map(_._2) - correctTarget).map {
      case l: HasLabel => FalseLit -> l
      case p: Property => NullLit(p.cypherType) -> p
      case other => throw RecordHeaderException(s"$correctTarget can only own HasLabel and Property but found $other")
    }

    (childMapping ++ missingMapping).foldLeft(physicalOp) {
      case (acc, (f, t)) =>
        val targetHeader = acc.header.withExpr(t)
        producer.planCopyColumn(acc, f, t, targetHeader)
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
      producer.planFilter(path, filterExpr, path.header)
    } else {
      producer.planJoin(path, physicalTargetOp, Seq(expr -> target), path.header join physicalTargetOp.header)
    }
  }
}

class DirectedVarLengthExpandPlanner[
O <: FlatRelationalTable[O],
K <: PhysicalOperator[O, A, P, I],
A <: RelationalCypherRecords[O],
P <: PropertyGraph,
I <: RuntimeContext[O, A, P]](
  override val source: Var,
  override val edgeScan: Var,
  override val innerNode: Var,
  override val target: Var,
  override val lower: Int,
  override val upper: Int,
  override val sourceOp: FlatOperator,
  override val edgeScanOp: FlatOperator,
  override val innerNodeOp: FlatOperator,
  override val targetOp: FlatOperator,
  override val header: RecordHeader,
  override val isExpandInto: Boolean
)(
  override val planner: PhysicalPlanner[O, K, A, P, I],
  override implicit val context: PhysicalPlannerContext[O, K, A]
) extends VarLengthExpandPlanner[O, K, A, P, I] {

  private val expandCacheOp = producer.planJoin(
    physicalInnerNodeOp, physicalEdgeScanOp,
    Seq(innerNode -> edgeScanOp.header.startNodeFor(edgeScan)),
    innerNodeOp.header join edgeScanOp.header
  )

  override def plan: K = {
    // Iteratively expand beginning from startOp with cacheOp
    val expandOps = (2 to upper).foldLeft(Seq(init(Outbound) -> Seq(startEdgeScan))) {
      case (acc, i) =>
        val (last, edgeVars) = acc.last
        val (next, nextEdge) = expand(i, last, expandCacheOp, Outbound, edgeVars)
        acc :+ (next -> (edgeVars :+ nextEdge))
    }.filter(_._2.size >= lower)

    // Join target nodes on expand ops
    val withTargetOps = expandOps.map { case (op, edges) => addTargetOps(op, edges.last, Outbound) }

    finalize(withTargetOps)
  }
}

class UndirectedVarLengthExpandPlanner[
O <: FlatRelationalTable[O],
K <: PhysicalOperator[O, A, P, I],
A <: RelationalCypherRecords[O],
P <: PropertyGraph,
I <: RuntimeContext[O, A, P]](
  override val source: Var,
  override val edgeScan: Var,
  override val innerNode: Var,
  override val target: Var,
  override val lower: Int,
  override val upper: Int,
  override val sourceOp: FlatOperator,
  override val edgeScanOp: FlatOperator,
  override val innerNodeOp: FlatOperator,
  override val targetOp: FlatOperator,
  override val header: RecordHeader,
  override val isExpandInto: Boolean
)(
  override val planner: PhysicalPlanner[O, K, A, P, I],
  override implicit val context: PhysicalPlannerContext[O, K, A]
) extends VarLengthExpandPlanner[O, K, A, P, I] {

  private val expandCacheOp = producer.planJoin(
    physicalInnerNodeOp, physicalEdgeScanOp,
    Seq(innerNode -> edgeScanOp.header.startNodeFor(edgeScan)),
    innerNodeOp.header join edgeScanOp.header
  )

  private val reversedExpandCacheOp = producer.planJoin(
    physicalInnerNodeOp, physicalEdgeScanOp,
    Seq(innerNode -> edgeScanOp.header.endNodeFor(edgeScan)),
    innerNodeOp.header join edgeScanOp.header
  )

  override def plan: K = {

    val outStartOp = init(Outbound)
    val inStartOp = init(Inbound)

    // Iteratively expand beginning from startOp with cacheOp
    val expandOps = (2 to upper).foldLeft(Seq((outStartOp -> inStartOp) -> Seq(startEdgeScan))) {
      case (acc, i) =>
        val ((last, lastRevered), edgeVars) = acc.last

        val (outOut, nextEdge) = expand(i, last, expandCacheOp, Outbound, edgeVars)
        val (outIn, _) = expand(i, last, reversedExpandCacheOp, Outbound, edgeVars)
        val (inOut, _) = expand(i, lastRevered, expandCacheOp, Inbound, edgeVars)
        val (inIn, _) = expand(i, lastRevered, reversedExpandCacheOp, Inbound, edgeVars)
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