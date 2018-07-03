package org.opencypher.okapi.relational.impl.operators

import org.opencypher.okapi.api.graph.{CypherSession, QualifiedGraphName}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherInteger
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, SchemaException}
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.physical.RuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.impl.physical._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.trees.AbstractTreeNode

abstract class RelationalOperator[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]] extends AbstractTreeNode[RelationalOperator[O, K, A, P, I]] {

  type TagStrategy = Map[QualifiedGraphName, Map[Int, Int]]

  def tagStrategy: TagStrategy = Map.empty

  def header: RecordHeader = children.head.header

  def _table: O = children.head.table

  implicit def context: I = children.head.context

  implicit def session: CypherSession = context.session

  def graph: P = children.head.graph

  def graphName: QualifiedGraphName = children.head.graphName

  def returnItems: Option[Seq[Var]] = children.head.returnItems

  protected def resolve(qualifiedGraphName: QualifiedGraphName)(implicit context: I): P =
    context.resolveGraph(qualifiedGraphName)

  protected def resolveTags(qgn: QualifiedGraphName)(implicit context: I): Set[Int] =
    resolve(qgn).tags

  def table: O = {
    val t = _table

    if (t.physicalColumns.toSet != header.columns) {
      // Ensure no duplicate columns in initialData
      val initialDataColumns = t.physicalColumns

      val duplicateColumns = initialDataColumns.groupBy(identity).collect {
        case (key, values) if values.size > 1 => key
      }

      if (duplicateColumns.nonEmpty)
        throw IllegalArgumentException(
          s"${getClass.getSimpleName}: a table with distinct columns",
          s"a table with duplicate columns: ${initialDataColumns.sorted.mkString("[", ", ", "]")}")

      // Verify that all header column names exist in the data
      val headerColumnNames = header.columns
      val dataColumnNames = t.physicalColumns.toSet
      val missingTableColumns = headerColumnNames -- dataColumnNames
      if (missingTableColumns.nonEmpty) {
        throw IllegalArgumentException(
          s"${getClass.getSimpleName}: data with columns ${header.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
          s"data with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}"
        )
      }
      // TODO: uncomment and fix expectations
      //      val missingHeaderColumns = dataColumnNames -- headerColumnNames
      //      if (missingHeaderColumns.nonEmpty) {
      //        throw IllegalArgumentException(
      //          s"data with columns ${header.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
      //          s"data with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}"
      //        )
      //      }

      // Verify column types
      header.expressions.foreach { expr =>
        val tableType = t.columnType(header.column(expr))
        val headerType = expr.cypherType
        // if the type in the data doesn't correspond to the type in the header we fail
        // except: we encode nodes, rels and integers with the same data type, so we can't fail
        // on conflicts when we expect entities (alternative: change reverse-mapping function somehow)

        headerType match {
          case _: CTNode if tableType == CTInteger =>
          case _: CTNodeOrNull if tableType == CTInteger =>
          case _: CTRelationship  if tableType == CTInteger =>
          case _: CTRelationshipOrNull if tableType == CTInteger =>
          case _ if tableType == headerType =>
          case _ => throw IllegalArgumentException(
            s"${getClass.getSimpleName}: data matching header type $headerType for expression $expr", tableType)
        }
      }
    }
    t
  }
}

// Leaf

final case class Start[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](qgn: QualifiedGraphName, recordsOpt: Option[A])
  (implicit override val context: I) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = recordsOpt.map(_.header).getOrElse(RecordHeader.empty)

//  override lazy val _table: O = recordsOpt.map(_.table).getOrElse(CAPSRecords.unit().table)
  override lazy val _table: O = recordsOpt.map(_.table).getOrElse(table.unit)

  override lazy val graph: P = resolve(qgn)

  override lazy val graphName: QualifiedGraphName = qgn

  override lazy val returnItems: Option[Seq[Var]] = None

  override def toString: String = {
    val graphArg = qgn.toString
    val recordsArg = recordsOpt.map(_.toString)
    val allArgs = List(recordsArg, graphArg).mkString(", ")
    s"Start($allArgs)"
  }

}

// Unary

final case class Cache[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K) extends RelationalOperator[O, K, A, P, I] {

  override lazy val _table: O = context.cache.getOrElse(in, {
    in.table.cache
    context.cache(in) = in.table
    in.table
  })

}

final case class NodeScan[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, v: Var) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = in.graph.schema.headerForNode(v)

  // TODO: replace with NodeVar
  override lazy val _table: O = {
    val nodeTable = in.graph.nodes(v.name, v.cypherType.asInstanceOf[CTNode]).table

    if (header.columns != nodeTable.physicalColumns.toSet) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed columns based on graph schema: ${header.columns.toSeq.sorted.mkString(", ")}
           |  - Actual columns in scan table: ${nodeTable.physicalColumns.sorted.mkString(", ")}
        """.stripMargin)
    }
    nodeTable
  }
}

final case class RelationshipScan[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, v: Var) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = in.graph.schema.headerForRelationship(v)

  // TODO: replace with RelationshipVar
  override lazy val _table: O = {
    val relTable = in.graph.relationships(v.name, v.cypherType.asInstanceOf[CTRelationship]).table

    if (header.columns != relTable.physicalColumns.toSet) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed columns based on graph schema: ${header.columns.toSeq.sorted.mkString(", ")}
           |  - Actual columns in scan table: ${relTable.physicalColumns.sorted.mkString(", ")}
        """.stripMargin)
    }
    relTable
  }
}

final case class Alias[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, aliases: Seq[AliasExpr]) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = in.header.withAlias(aliases: _*)
}

final case class Add[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, expr: Expr) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = {
    if (in.header.contains(expr)) {
      expr match {
        case a: AliasExpr => in.header.withAlias(a)
        case _ => in.header
      }
    } else {
      expr match {
        case a: AliasExpr => in.header.withExpr(a.expr).withAlias(a)
        case _ => in.header.withExpr(expr)
      }
    }
  }

  override lazy val _table: O = {
    if (in.header.contains(expr)) {
      in.table
    } else {
      in.table.withColumn(header.column(expr), expr)(header, context.parameters)
    }
  }
}

final case class AddInto[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, add: Expr, into: Expr) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = in.header.withExpr(into)

  override lazy val _table: O = in.table.withColumn(header.column(into), add)(header, context.parameters)
}

final case class Drop[
T <: Expr,
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, exprs: Set[T]) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = in.header -- exprs

  private lazy val columnsToDrop = in.header.columns -- header.columns

  override lazy val _table: O = {
    if (columnsToDrop.nonEmpty) {
      in.table.drop(columnsToDrop.toSeq: _*)
    } else {
      in.table
    }
  }
}

final case class RenameColumns[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, renameExprs: Map[Expr, String]) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = renameExprs.foldLeft(in.header) {
    case (currentHeader, (expr, newColumn)) => currentHeader.withColumnRenamed(expr, newColumn)
  }

  override lazy val _table: O = renameExprs.foldLeft(in.table) {
    case (currentTable, (expr, newColumn)) => currentTable.withColumnRenamed(in.header.column(expr), newColumn)
  }
}

final case class Filter[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, expr: Expr) extends RelationalOperator[O, K, A, P, I] {

  override lazy val _table: O = in.table.filter(expr)(header, context.parameters)
}

final case class ReturnGraph[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = RecordHeader.empty

  override lazy val _table: O = in.table.empty()
}

final case class Select[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, expressions: List[Expr]) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = {
    val aliasExprs = expressions.collect { case a: AliasExpr => a }
    val headerWithAliases = in.header.withAlias(aliasExprs: _*)
    headerWithAliases.select(expressions: _*)
  }

  override lazy val _table: O = {
    in.table.select(expressions.map(header.column).distinct: _*)
  }

  override lazy val returnItems: Option[Seq[Var]] = Some(expressions.flatMap(_.owner).collect { case e: Var => e }.distinct)
}

final case class Distinct[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, fields: Set[Var]) extends RelationalOperator[O, K, A, P, I] {

  override lazy val _table: O = in.table.distinct(fields.flatMap(header.expressionsFor).map(header.column).toSeq: _*)

}

final case class SimpleDistinct[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K) extends RelationalOperator[O, K, A, P, I] {

  override lazy val _table: O = in.table.distinct
}

final case class Aggregate[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](
  in: K,
  aggregations: Set[(Var, Aggregator)],
  group: Set[Var]
) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = in.header.select(group).withExprs(aggregations.map(_._1))

  override lazy val _table: O = {
    val preparedAggregations = aggregations.map { case (v, agg) => agg -> (header.column(v) -> v.cypherType) }
    in.table.group(group, preparedAggregations)(in.header, context.parameters)
  }
}

final case class OrderBy[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, sortItems: Seq[SortItem[Expr]]) extends RelationalOperator[O, K, A, P, I] {

  override lazy val _table: O = {
    val tableSortItems: Seq[(String, Order)] = sortItems.map {
      case Asc(expr) => header.column(expr) -> Ascending
      case Desc(expr) => header.column(expr) -> Descending
    }
    in.table.orderBy(tableSortItems: _*)
  }
}

final case class Skip[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, expr: Expr) extends RelationalOperator[O, K, A, P, I] {

  override lazy val _table: O = {
    val skip: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(l) => l
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal or parameter", other)
    }
    in.table.skip(skip)
  }
}

final case class Limit[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, expr: Expr) extends RelationalOperator[O, K, A, P, I] {

  override lazy val _table: O = {
    val limit: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(v) => v
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal", other)
    }
    in.table.limit(limit)
  }
}

final case class EmptyRecords[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, fields: Set[Var] = Set.empty) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = RecordHeader.from(fields)

  override lazy val _table: O = in.table.empty(header)
}

final case class FromGraph[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](in: K, logicalGraph: LogicalCatalogGraph) extends RelationalOperator[O, K, A, P, I] {

  override def graph: P = resolve(logicalGraph.qualifiedGraphName)

  override def graphName: QualifiedGraphName = logicalGraph.qualifiedGraphName

}

// Binary

final case class Join[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](
  lhs: K,
  rhs: K,
  joinExprs: Seq[(Expr, Expr)] = Seq.empty,
  joinType: JoinType = CrossJoin
) extends RelationalOperator[O, K, A, P, I] {

  override lazy val header: RecordHeader = lhs.header join rhs.header

  override lazy val _table: O = {

    val lhsTable = lhs.table
    val rhsTable = rhs.table

    // TODO: move conflict resolution to relational planner
    val conflictFreeRhs = if (lhsTable.physicalColumns.toSet ++ rhsTable.physicalColumns.toSet != header.columns) {
      val renameColumns = rhs.header.expressions
        .filter(expr => rhs.header.column(expr) != header.column(expr))
        .map { expr => expr -> header.column(expr) }.toSeq
      RenameColumns[O, K, A, P, I](rhs, renameColumns.toMap)
    } else {
      rhs
    }

    val joinCols = joinExprs.map { case (l, r) => header.column(l) -> conflictFreeRhs.header.column(r) }
    lhs.table.join(conflictFreeRhs.table, joinType, joinCols: _*)
  }
}

/**
  * Computes the union of the two input operators. The two inputs must have identical headers.
  * This operation does not remove duplicates.
  *
  * The output header of this operation is identical to the input headers.
  *
  * @param lhs the first operand
  * @param rhs the second operand
  */
// TODO: rename to UnionByName
// TODO: refactor to n-ary operator (i.e. take List[PhysicalOperator] as input)
final case class TabularUnionAll[
O <: FlatRelationalTable[O],
K <: RelationalOperator[O, K, A, P, I],
A <: RelationalCypherRecords[O],
P <: RelationalCypherGraph[O],
I <: RuntimeContext[O, K, A, P, I]](lhs: K, rhs: K) extends RelationalOperator[O, K, A, P, I] {

  override lazy val _table: O = {
    val lhsTable = lhs.table
    val rhsTable = rhs.table

    val leftColumns = lhsTable.physicalColumns
    val rightColumns = rhsTable.physicalColumns

    if (leftColumns.size != rightColumns.size) {
      throw IllegalArgumentException("same number of columns", s"left: $leftColumns right: $rightColumns")
    }
    if (leftColumns.toSet != rightColumns.toSet) {
      throw IllegalArgumentException("same column names", s"left: $leftColumns right: $rightColumns")
    }

    val orderedRhsTable = if (leftColumns != rightColumns) {
      rhsTable.select(leftColumns: _*)
    } else {
      rhsTable
    }

    lhsTable.unionAll(orderedRhsTable)
  }
}



// N-ary

//final case class GraphUnionAll[
//O <: FlatRelationalTable[O],
//K <: RelationalOperator[O, K, A, P, I],
//A <: RelationalCypherRecords[O],
//P <: RelationalCypherGraph[O],
//I <: RuntimeContext[O, K, A, P, I]](inputs: List[K], qgn: QualifiedGraphName)
//  extends RelationalOperator[O, K, A, P, I] {
//  require(inputs.nonEmpty, "GraphUnionAll requires at least one input")
//
//  override lazy val tagStrategy = {
//    computeRetaggings(inputs.map(r => r.graphName -> r.graph.tags).toMap)
//  }
//
//  override lazy val graphName = qgn
//
//  override lazy val graph = {
//    val graphWithTagStrategy = inputs.map(i => i.graph -> tagStrategy(i.graphName)).toMap
//    CAPSUnionGraph(graphWithTagStrategy)
//  }
//
//}
