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
package org.opencypher.okapi.relational.impl.table

import org.opencypher.okapi.api.types.{CTNode, CTNodeOrNull, CTRelationship, CTRelationshipOrNull}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.util.TablePrinter
import org.opencypher.okapi.ir.api.RelType
import org.opencypher.okapi.ir.api.expr._

object RecordHeader {

  def empty: RecordHeader = RecordHeader(Map.empty)

  def from[T <: Expr](expr: T, exprs: T*): RecordHeader = empty.withExprs(expr, exprs: _*)

  def from[T <: Expr](exprs: Set[T]): RecordHeader = empty.withExprs(exprs)

  def from[T <: Expr](exprs: Seq[T]): RecordHeader = from(exprs.head, exprs.tail: _*)

}

case class RecordHeader(exprToColumn: Map[Expr, String]) {

  // ==============
  // Lookup methods
  // ==============

  def expressions: Set[Expr] = exprToColumn.keySet

  def vars: Set[Var] = expressions.collect { case v: Var => v }

  def columns: Set[String] = exprToColumn.values.toSet

  def isEmpty: Boolean = exprToColumn.isEmpty

  def contains(expr: Expr): Boolean = exprToColumn.contains(expr)

  def getColumn(expr: Expr): Option[String] = exprToColumn.get(expr)

  def column(expr: Expr): String =
    exprToColumn.getOrElse(expr, throw IllegalArgumentException(s"Header does not contain a column for $expr.\n\t${this.toString}"))

  def ownedBy(expr: Var): Set[Expr] = {
    exprToColumn.keys.filter(e => e.owner.contains(expr)).toSet
  }

  def expressionsFor(expr: Expr): Set[Expr] = {
    expr match {
      case v: Var => ownedBy(v)
      case e if exprToColumn.contains(e) => Set(e)
      case _ => Set.empty
    }
  }

  def expressionsFor(column: String): Set[Expr] = {
    exprToColumn.collect { case (k, v) if v == column => k }.toSet
  }

  def aliasesFor(expr: Expr): Set[Var] = {
    val aliasesFromHeader: Set[Var] = getColumn(expr) match {
      case None => Set.empty
      case Some(col) => exprToColumn.collect { case (k: Var, v) if v == col => k }.toSet
    }

    val aliasesFromParam: Set[Var] = expr match {
      case v: Var => Set(v)
      case _ => Set.empty
    }

    aliasesFromHeader ++ aliasesFromParam
  }

  // ===================
  // Convenience methods
  // ===================

  def idExpressions(): Set[Expr] = {
    exprToColumn.keySet.collect {
      case n if n.cypherType.subTypeOf(CTNode).isTrue => n
      case r if r.cypherType.subTypeOf(CTRelationship).isTrue => r
    }
  }

  def idExpressions(v: Var): Set[Expr] = idExpressions().filter(_.owner.get == v)

  def idColumns(): Set[String] = idExpressions().map(column)

  def idColumns(v: Var): Set[String] = idExpressions(v).map(column)

  def labelsFor(n: Var): Set[HasLabel] = {
    ownedBy(n).collect {
      case l: HasLabel => l
    }
  }

  def typesFor(r: Var): Set[HasType] = {
    ownedBy(r).collect {
      case t: HasType => t
    }
  }

  def startNodeFor(r: Var): StartNode = {
    ownedBy(r).collectFirst {
      case s: StartNode => s
    }.get
  }

  def endNodeFor(r: Var): EndNode = {
    ownedBy(r).collectFirst {
      case e: EndNode => e
    }.get
  }

  def propertiesFor(v: Var): Set[Property] = {
    ownedBy(v).collect {
      case p: Property => p
    }
  }

  def node(name: Var): Set[Expr] = {
    exprToColumn.keys.collect {
      case n: Var if name == n => n
      case h@HasLabel(n: Var, _) if name == n => h
      case p@Property(n: Var, _) if name == n => p
    }.toSet
  }

  def entityVars: Set[Var] = nodeVars ++ relationshipVars

  def nodeVars: Set[Var] = {
    exprToColumn.keySet.collect {
      case v: Var if v.cypherType.subTypeOf(CTNodeOrNull).isTrue => v
    }
  }

  def relationshipVars: Set[Var] = {
    exprToColumn.keySet.collect {
      case v: Var if v.cypherType.subTypeOf(CTRelationshipOrNull).isTrue => v
    }
  }

  def nodesForType(nodeType: CTNode): Set[Var] = {
    // and semantics
    val requiredLabels = nodeType.labels

    nodeVars.filter { nodeVar =>
      val physicalLabels = labelsFor(nodeVar).map(_.label.name)
      val logicalLabels = nodeVar.cypherType match {
        case CTNode(labels, _) => labels
        case _ => Set.empty[String]
      }
      requiredLabels.subsetOf(physicalLabels ++ logicalLabels)
    }
  }

  def relationshipsForType(relType: CTRelationship): Set[Var] = {
    // or semantics
    val possibleTypes = relType.types

    relationshipVars.filter { relVar =>
      val physicalTypes = typesFor(relVar).map {
        case HasType(_, RelType(name)) => name
      }
      val logicalTypes = relVar.cypherType match {
        case CTRelationship(types, _) => types
        case _ => Set.empty[String]
      }
      possibleTypes.isEmpty || (physicalTypes ++ logicalTypes).exists(possibleTypes.contains)
    }
  }

  // ================
  // Mutation methods
  // ================

  def select[T <: Expr](exprs: T*): RecordHeader = select(exprs.toSet)

  def select[T <: Expr](exprs: Set[T]): RecordHeader = {
    val selectExpressions = exprs.flatMap { e: Expr =>
      e match {
        case v: Var => ownedBy(v)
        case nonVar => Set(nonVar)
      }
    }
    val selectMappings = exprToColumn.filterKeys(selectExpressions.contains)
    RecordHeader(selectMappings)
  }

  def withColumnsRenamed[T <: Expr](renamings: Map[T, String]): RecordHeader = {
    renamings.foldLeft(this) {
      case (currentHeader, (expr, newColumn)) => currentHeader.withColumnRenamed(expr, newColumn)
    }
  }

  def withColumnRenamed[T <: Expr](expr: T, newColumn: String): RecordHeader = {
    withColumnRenamed(column(expr), newColumn)
  }

  def withColumnRenamed(oldColumn: String, newColumn: String): RecordHeader = {
    val exprs = expressionsFor(oldColumn)
    copy(exprToColumn ++ exprs.map(_ -> newColumn))
  }

  def withExpr(expr: Expr): RecordHeader = {
    exprToColumn.get(expr) match {
      case Some(_) => this

      case None =>
        val newColumnName = (expr.toString.hashCode & Int.MaxValue).toString

        // Aliases for (possible) owner of expr need to be updated as well
        val exprsToAdd: Set[Expr] = expr.owner match {
          case None => Set(expr)

          case Some(exprOwner) => aliasesFor(exprOwner).map(alias => expr.withOwner(alias))
        }

        exprsToAdd.foldLeft(this) {
          case (current, e) => current.addExprToColumn(e, newColumnName)
        }
    }
  }

  def withExprs[T <: Expr](expr: T, exprs: T*): RecordHeader = (expr +: exprs).foldLeft(this)(_ withExpr _)

  def withExprs[T <: Expr](exprs: Set[T]): RecordHeader = {
    if (exprs.isEmpty) {
      this
    } else {
      withExprs(exprs.head, exprs.tail.toSeq: _*)
    }
  }

  def withAlias(aliases: AliasExpr*): RecordHeader = aliases.foldLeft(this){
    case (currentHeader, alias) => currentHeader.withAlias(alias)
  }

  def withAlias(expr: AliasExpr): RecordHeader = {
    val to = expr.expr
    val alias = expr.alias
    to match {
      // Entity case
      case _: Var if exprToColumn.contains(to) =>
        expressionsFor(to).foldLeft(this) {
          case (current, nextExpr) => current.addExprToColumn(nextExpr.withOwner(alias), exprToColumn(nextExpr))
        }

      // Non-entity case
      case e if exprToColumn.contains(e) => addExprToColumn(alias, exprToColumn(e))

      // No expression to alias
      case other => throw IllegalArgumentException(s"An expression in $this", s"Unknown expression $other")
    }
  }

  def join(other: RecordHeader): RecordHeader = {
    val expressionOverlap = expressions.intersect(other.expressions)
    if(expressionOverlap.nonEmpty) {
      throw IllegalArgumentException("two headers with non overlapping expressions", s"overlapping expressions: $expressionOverlap")
    }

    val cleanOther = if (columns.intersect(other.columns).nonEmpty) {
      val (rename, keep) = other.expressions.partition(e => this.columns.contains(other.column(e)))
      val withKept = keep.foldLeft(RecordHeader.empty){
        case (acc, next) => acc.addExprToColumn(next, other.column(next))
      }

      rename.groupBy(other.column).mapValues(_.toSeq.sorted).foldLeft(withKept) {
        case (acc, (_, exprs)) =>
          val add = acc.withExpr(exprs.head)
          val newColumn = add.column(exprs.head)
          exprs.tail.foldLeft(add) {
            case (acc2, expr) => acc2.addExprToColumn(expr, newColumn)
          }
      }
    } else other

    this ++ cleanOther
  }

  def ++(other: RecordHeader): RecordHeader = copy(exprToColumn = exprToColumn ++ other.exprToColumn)

  def --[T <: Expr](expressions: Set[T]): RecordHeader = {
    val expressionToRemove = expressions.flatMap(expressionsFor)
    val updatedExprToColumn = exprToColumn.filterNot { case (e, _) => expressionToRemove.contains(e) }
    copy(exprToColumn = updatedExprToColumn)
  }

  def addExprToColumn(expr: Expr, columnName: String): RecordHeader = {
    copy(exprToColumn = exprToColumn + (expr -> columnName))
  }

  def pretty: String = {
    val formatCell: String => String = s => s"'$s'"
    val (header, row) = exprToColumn
      .toSeq
      .sortBy(_._2)
      .map { case (expr, column) => expr.toString -> column }
      .unzip
    TablePrinter.toTable(header, Seq(row))(formatCell)
  }

}

