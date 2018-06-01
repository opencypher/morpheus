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

import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._

object RecordHeaderNew {

  def empty: RecordHeaderNew = RecordHeaderNew(Map.empty)
}

case class RecordHeaderNew(exprToColumn: Map[Expr, String]) {

  def getColumn(expr: Expr): Option[String] = exprToColumn.get(expr)

  def column(expr: Expr): String =
    exprToColumn.getOrElse(expr, throw IllegalArgumentException(s"Header does not contain a column for $expr: ${this.toString}"))

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

  def idColumns: Set[String] = {
    exprToColumn.keySet.collect {
      case n if n.cypherType.superTypeOf(CTNode).isTrue => n
      case r if r.cypherType.superTypeOf(CTRelationship).isTrue => r
    }.map(column)
  }

  def node(name: Var): Set[Expr] = {
    exprToColumn.keys.collect {
      case n: Var if name == n => n
      case h@HasLabel(n: Var, _) if name == n => h
      case p@Property(n: Var, _) if name == n => p
    }.toSet
  }

  def withExpr(expr: Expr): RecordHeaderNew = {
    exprToColumn.get(expr) match {
      case Some(_) => this

      case None =>
        val newColumnName = ColumnNamer.of(expr)

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

  def withExprs(expr: Expr, exprs: Expr*): RecordHeaderNew = (expr +: exprs).foldLeft(this)(_ withExpr _)

  def withExprs(exprs: Set[Expr]): RecordHeaderNew = withExprs(exprs.head, exprs.tail.toSeq: _*)

  def withAlias(alias: Var, to: Expr): RecordHeaderNew = {
    require(
      alias.cypherType.superTypeOf(to.cypherType).isTrue,
      s"CypherType of expression $to cannot be assigned to CypherType of alias $alias")

    to match {
      // Entity case
      case _: Var if exprToColumn.contains(to) =>
        expressionsFor(to).foldLeft(this) {
          case (current, nextExpr) => current.addExprToColumn(nextExpr.withOwner(alias), exprToColumn(nextExpr))
        }

      // Non-entity case
      case expr if exprToColumn.contains(expr) => addExprToColumn(alias, exprToColumn(expr))

      // No expression to alias
      case other => throw IllegalArgumentException(s"An expression in $this", s"Unknown expression $other")
    }
  }

  def ++(other: RecordHeaderNew): RecordHeaderNew = copy(exprToColumn = exprToColumn ++ other.exprToColumn)

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

  protected def addExprToColumn(expr: Expr, columnName: String): RecordHeaderNew = {
    copy(exprToColumn = exprToColumn + (expr -> columnName))
  }

  def pretty: String = exprToColumn.toSeq.sortBy(_._2).mkString("\n")

}

