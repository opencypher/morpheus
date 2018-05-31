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

import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._

import scala.annotation.tailrec


object RecordHeaderNew {

  def empty: RecordHeaderNew = RecordHeaderNew(Map.empty, Map.empty)

}

case class RecordHeaderNew(
  exprToColumn: Map[Expr, String],
  aliasToExpr: Map[Var, Expr]
) {

  @tailrec
  final def column(expr: Expr): String = {
    exprToColumn.get(expr) match {
      case Some(col) => col
      case None =>
        expr match {
          case v: Var => column(aliasToExpr(v))
          case _ => throw IllegalArgumentException(s"Header does not contain a column for $expr: ${this.toString}")
        }
    }
  }

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

  def node(name: Var): Set[Expr] = {
    exprToColumn.keys.collect {
      case n: Var if name == n => n
      case h@HasLabel(n: Var, _) if name == n => h
      case p@Property(n: Var, _) if name == n => p
    }.toSet
  }

  def withExpr(expr: Expr): RecordHeaderNew = {
    val existing = exprToColumn.get(expr)
    existing match {
      case Some(_) => this
      case None => copy(exprToColumn = exprToColumn + (expr -> ColumnNamer.of(expr)))
    }
  }

  protected def addExprToColumn(expr: Expr, columnName: String): RecordHeaderNew = {
    copy(exprToColumn = exprToColumn + (expr -> columnName))
  }

  protected def addAliasToExpr(alias: Var, expr: Expr): RecordHeaderNew = {
    copy(aliasToExpr = aliasToExpr + (alias -> expr))
  }

  def withAlias(alias: Var, to: Expr): RecordHeaderNew = {
    val toAlias = expressionsFor(to)
    val updated = toAlias.foldLeft(this) { case (current, nextExpr) =>
      if (nextExpr.isEntityExpression) {
        current.addExprToColumn(nextExpr.withOwner(alias), exprToColumn(nextExpr))
      } else {
        current.addAliasToExpr(alias, nextExpr)
      }
    }
    if (!updated.exprToColumn.contains(alias)) {
      updated.addAliasToExpr(alias, to)
    } else {
      updated
    }
  }

}

