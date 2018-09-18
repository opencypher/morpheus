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
package org.opencypher.okapi.relational.impl.operators

import cats.data.NonEmptyList
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.{CTInteger, _}
import org.opencypher.okapi.api.value.CypherValue.CypherInteger
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl.{LogicalCatalogGraph, LogicalPatternGraph}
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}
import org.opencypher.okapi.relational.impl.operators.TagStrategy._
import org.opencypher.okapi.relational.impl.planning._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.trees.AbstractTreeNode
import org.opencypher.okapi.relational.impl.RelationalConverters._

import scala.reflect.runtime.universe.TypeTag

object TagStrategy {

  type TagStrategy = Map[QualifiedGraphName, Map[Int, Int]]

  def empty = Map.empty[QualifiedGraphName, Map[Int, Int]]

  def apply(tuple: (QualifiedGraphName, Map[Int, Int])*): Map[QualifiedGraphName, Map[Int, Int]] = {
    tuple.toMap
  }
}

abstract class RelationalOperator[T <: Table[T] : TypeTag] extends AbstractTreeNode[RelationalOperator[T]] {

  def tagStrategy: TagStrategy = Map.empty

  def header: RecordHeader = children.head.header

  def _table: T = children.head.table

  implicit def context: RelationalRuntimeContext[T] = children.head.context

  implicit def session: RelationalCypherSession[T] = context.session

  def graph: RelationalCypherGraph[T] = children.head.graph

  def graphName: QualifiedGraphName = children.head.graphName

  def returnItems: Option[Seq[Var]] = children.head.returnItems

  protected def resolve(qualifiedGraphName: QualifiedGraphName)
    (implicit context: RelationalRuntimeContext[T]): RelationalCypherGraph[T] =
    context.resolveGraph(qualifiedGraphName)

  protected def resolveTags(qgn: QualifiedGraphName)(implicit context: RelationalRuntimeContext[T]): Set[Int] =
    resolve(qgn).tags

  def table: T = {
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
          s"${getClass.getSimpleName}: table with columns ${header.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
          s"""|table with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}
              |column(s) ${missingTableColumns.mkString(", ")} are missing in the table
           """.stripMargin
        )
      }

      val missingHeaderColumns = dataColumnNames -- headerColumnNames
      if (missingHeaderColumns.nonEmpty) {
        throw IllegalArgumentException(
          s"data with columns ${header.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
          s"data with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}"
        )
      }

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
          case _: CTRelationship if tableType == CTInteger =>
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

object Start {

  def from[T <: Table[T] : TypeTag](header: RecordHeader, table: T)
    (implicit context: RelationalRuntimeContext[T]): Start[T] = {
    Start(context.session.emptyGraphQgn, Some(context.session.records.from(header, table)))
  }

  def apply[T <: Table[T] : TypeTag](records: RelationalCypherRecords[T])
    (implicit context: RelationalRuntimeContext[T]): Start[T] = {
    Start(context.session.emptyGraphQgn, Some(records))
  }
}

final case class Start[T <: Table[T] : TypeTag](
  qgn: QualifiedGraphName,
  maybeRecords: Option[RelationalCypherRecords[T]] = None,
  override val tagStrategy: TagStrategy = TagStrategy.empty
)(implicit override val context: RelationalRuntimeContext[T], override val tt: TypeTag[RelationalOperator[T]])
  extends RelationalOperator[T] {

  override lazy val header: RecordHeader = maybeRecords.map(_.header).getOrElse(RecordHeader.empty)

  override lazy val _table: T = maybeRecords.map(_.table).getOrElse {
    session.records.unit().table
  }

  override lazy val graph: RelationalCypherGraph[T] = resolve(qgn)

  override lazy val graphName: QualifiedGraphName = qgn

  override lazy val returnItems: Option[Seq[Var]] = None

  override def toString: String = {
    val graphArg = qgn.toString
    val recordsArg = maybeRecords.map(_.toString)
    val allArgs = List(recordsArg, graphArg).mkString(", ")
    s"Start($allArgs)"
  }

}

// Unary

/**
  * Cache is a marker operator that indicates that its child operator is used multiple times within the query.
  */
final case class Cache[T <: Table[T] : TypeTag](in: RelationalOperator[T])
   extends RelationalOperator[T] {

  override lazy val _table: T = in._table.cache()

}

final case class SwitchContext[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  override val context: RelationalRuntimeContext[T]
) extends RelationalOperator[T]


final case class Alias[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  aliases: Seq[AliasExpr]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.header.withAlias(aliases: _*)
}

final case class Add[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  exprs: List[Expr]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = {
    exprs.foldLeft(in.header) { case (aggHeader, expr) =>
      if (aggHeader.contains(expr)) {
        expr match {
          case a: AliasExpr => aggHeader.withAlias(a)
          case _ => aggHeader
        }
      } else {
        expr match {
          case a: AliasExpr => aggHeader.withExpr(a.expr).withAlias(a)
          case _ => aggHeader.withExpr(expr)
        }
      }
    }
  }

  override lazy val _table: T = {
    // TODO check for equal nullability setting
    val physicalAdditions = exprs.filterNot(in.header.contains)
    if (physicalAdditions.isEmpty) {
      in.table
    } else {
      in.table.withColumns(physicalAdditions.map(expr => expr -> header.column(expr)): _*)(header, context.parameters)
    }
  }
}

final case class AddInto[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  valueIntoTuples: List[(Expr, Expr)]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = {
    valueIntoTuples.map(_._2).foldLeft(in.header)(_.withExpr(_))
  }

  override lazy val _table: T = {
    val valuesToColumnNames = valueIntoTuples.map { case (value, into) => value -> header.column(into) }
    in.table.withColumns(valuesToColumnNames: _*)(header, context.parameters)
  }
}

final case class Drop[E <: Expr, T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  exprs: Set[E]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.header -- exprs

  private lazy val columnsToDrop = in.header.columns -- header.columns

  override lazy val _table: T = {
    if (columnsToDrop.nonEmpty) {
      in.table.drop(columnsToDrop.toSeq: _*)
    } else {
      in.table
    }
  }
}

final case class RenameColumns[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  columnRenamings: Map[String, String]
) extends RelationalOperator[T] {

  private val actualRenamings: Map[String, String] = columnRenamings.filterNot { case (oldName, newName) => oldName == newName }

  override lazy val header: RecordHeader = actualRenamings.foldLeft(in.header) {
    case (currentHeader, (oldColumnName, newColumnName)) => currentHeader.withColumnRenamed(oldColumnName, newColumnName)
  }

  override lazy val _table: T = actualRenamings.foldLeft(in.table) {
    case (currentTable, (oldColumnName, newColumnName)) => currentTable.withColumnRenamed(oldColumnName, newColumnName)
  }
}

final case class Filter[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  expr: Expr
) extends RelationalOperator[T] {

  require(expr.cypherType.material == CTBoolean)

  override lazy val _table: T = in.table.filter(expr)(header, context.parameters)
}

final case class ReturnGraph[T <: Table[T] : TypeTag](in: RelationalOperator[T])
   extends RelationalOperator[T] {

  override lazy val header: RecordHeader = RecordHeader.empty

  override lazy val _table: T = session.records.empty().table
}

final case class Select[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  expressions: List[Expr]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.header.select(expressions: _*)

  override lazy val _table: T = {
    import Expr._
    val selectExpressions = expressions.map {
      case AliasExpr(_, alias) => alias
      case other => other
    }.flatMap(expr => header.expressionsFor(expr).toSeq.sorted)
    in.table.select(selectExpressions.map(header.column).distinct: _*)
  }

  override lazy val returnItems: Option[Seq[Var]] = Some(expressions.flatMap(_.owner).collect { case e: Var => e }.distinct)
}

final case class Distinct[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  fields: Set[Var]
) extends RelationalOperator[T] {

  override lazy val _table: T = in.table.distinct(fields.flatMap(header.expressionsFor).map(header.column).toSeq: _*)

}

final case class Aggregate[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  group: Set[Var],
  aggregations: Set[(Var, Aggregator)]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.header.select(group).withExprs(aggregations.map(_._1))

  override lazy val _table: T = {
    val preparedAggregations = aggregations.map { case (v, agg) => agg -> (header.column(v) -> v.cypherType) }
    in.table.group(group, preparedAggregations)(in.header, context.parameters)
  }
}

final case class OrderBy[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  sortItems: Seq[SortItem]
) extends RelationalOperator[T] {

  override lazy val _table: T = {
    val tableSortItems = sortItems.map {
      case Asc(expr) => expr -> Ascending
      case Desc(expr) => expr -> Descending
    }
    in.table.orderBy(tableSortItems: _*)(header, context.parameters)
  }
}

final case class Skip[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  expr: Expr
) extends RelationalOperator[T] {

  override lazy val _table: T = {
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

final case class Limit[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  expr: Expr
) extends RelationalOperator[T] {

  override lazy val _table: T = {
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

final case class EmptyRecords[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  fields: Set[Var] = Set.empty
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = RecordHeader.from(fields)

  override lazy val _table: T = session.records.empty(header).table
}

final case class FromCatalogGraph[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  logicalGraph: LogicalCatalogGraph
) extends RelationalOperator[T] {

  override def graph: RelationalCypherGraph[T] = resolve(logicalGraph.qualifiedGraphName)

  override def graphName: QualifiedGraphName = logicalGraph.qualifiedGraphName

}

// Binary

final case class Join[T <: Table[T] : TypeTag](
  lhs: RelationalOperator[T],
  rhs: RelationalOperator[T],
  joinExprs: Seq[(Expr, Expr)] = Seq.empty,
  joinType: JoinType
) extends RelationalOperator[T] {
  require(joinExprs.nonEmpty || joinType == CrossJoin, "Join type must either be 'CrossJoin' or join expressions may not be empty")
  require((lhs.header.expressions intersect rhs.header.expressions).isEmpty, "Join cannot join operators with overlapping expressions")
  require((lhs.header.columns intersect rhs.header.columns).isEmpty, "Join cannot join tables with column name collisions")

  override lazy val header: RecordHeader = lhs.header join rhs.header

  override lazy val _table: T = {
    val joinCols = joinExprs.map { case (l, r) => header.column(l) -> rhs.header.column(r) }
    lhs.table.join(rhs.table, joinType, joinCols: _*)
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
final case class TabularUnionAll[T <: Table[T] : TypeTag](
  lhs: RelationalOperator[T],
  rhs: RelationalOperator[T]
) extends RelationalOperator[T] {

  override lazy val _table: T = {
    val lhsTable = lhs.table
    val rhsTable = rhs.table

    val leftColumns = lhsTable.physicalColumns
    val rightColumns = rhsTable.physicalColumns

    val sortedLeftColumns = leftColumns.sorted.mkString(", ")
    val sortedRightColumns = rightColumns.sorted.mkString(", ")

    if (leftColumns.size != rightColumns.size) {
      throw IllegalArgumentException("same number of columns", s"left:  $sortedLeftColumns\n\tright: $sortedRightColumns")
    }

    if (leftColumns.toSet != rightColumns.toSet) {
      throw IllegalArgumentException("same column names", s"left:  $sortedLeftColumns\n\tright: $sortedRightColumns")
    }

    val orderedRhsTable = if (leftColumns != rightColumns) {
      rhsTable.select(leftColumns: _*)
    } else {
      rhsTable
    }

    lhsTable.unionAll(orderedRhsTable)
  }
}

final case class ConstructGraph[T <: Table[T] : TypeTag](
  in: RelationalOperator[T],
  constructedGraph: RelationalCypherGraph[T],
  override val graphName: QualifiedGraphName,
  override val tagStrategy: Map[QualifiedGraphName, Map[Int, Int]],
  construct: LogicalPatternGraph,
  override val context: RelationalRuntimeContext[T]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = RecordHeader.empty

  override lazy val _table: T = session.records.unit().table

  override def returnItems: Option[Seq[Var]] = None

  override lazy val graph: RelationalCypherGraph[T] = constructedGraph

  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }
}

// N-ary

final case class GraphUnionAll[T <: Table[T] : TypeTag](
  inputs: NonEmptyList[RelationalOperator[T]],
  qgn: QualifiedGraphName
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = RecordHeader.empty

  override lazy val _table: T = session.records.empty().table

  import org.opencypher.okapi.relational.api.tagging.TagSupport._

  override lazy val tagStrategy: Map[QualifiedGraphName, Map[Int, Int]] = computeRetaggings(inputs.toList.map(r => r.graphName -> r.graph.tags).toMap)

  override lazy val graphName: QualifiedGraphName = qgn

  override lazy val graph: RelationalCypherGraph[T] = {
    val graphWithTagStrategy = inputs.toList.map(i => i.graph -> tagStrategy(i.graphName)).toMap
    session.graphs.unionGraph(graphWithTagStrategy)
  }

}
