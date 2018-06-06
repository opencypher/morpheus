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
import org.opencypher.okapi.ir.api.RelType
import org.opencypher.okapi.ir.api.expr._

object RecordHeaderNew {

  def empty: RecordHeaderNew = RecordHeaderNew(Map.empty)

  def from[T <: Expr](expr: T, exprs: T*): RecordHeaderNew = empty.withExprs(expr, exprs: _*)

  def from[T <: Expr](exprs: Set[T]): RecordHeaderNew = empty.withExprs(exprs)

  def from[T <: Expr](exprs: Seq[T]): RecordHeaderNew = from(exprs.head, exprs.tail: _*)

}

case class RecordHeaderNew(exprToColumn: Map[Expr, String]) {

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
      case n if n.cypherType.superTypeOf(CTNode).isTrue => n
      case r if r.cypherType.superTypeOf(CTRelationship).isTrue => r
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

  def typeFor(r: Var): Option[HasType] = {
    ownedBy(r).collectFirst {
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
      case v: Var if v.cypherType.superTypeOf(CTNode).isTrue => v
    }
  }

  def relationshipVars: Set[Var] = {
    exprToColumn.keySet.collect {
      case v: Var if v.cypherType.superTypeOf(CTRelationship).isTrue => v
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
      val physicalType = typeFor(relVar) match {
        case Some(HasType(_, RelType(name))) => Set(name)
        case None => Set.empty[String]
      }
      val logicalTypes = relVar.cypherType match {
        case CTRelationship(types, _) => types
        case _ => Set.empty[String]
      }
      (physicalType ++ logicalTypes).exists(possibleTypes.contains)
    }
  }

  // ================
  // Mutation methods
  // ================

  def select[T <: Expr](exprs: T*): RecordHeaderNew = select(exprs.toSet)

  def select[T <: Expr](exprs: Set[T]): RecordHeaderNew = {
    val selectExpressions = exprs.flatMap { e: Expr =>
      e match {
        case v: Var => ownedBy(v)
        case nonVar => Set(nonVar)
      }
    }
    val selectMappings = exprToColumn.filterKeys(selectExpressions.contains)
    RecordHeaderNew(selectMappings)
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

  def withExprs[T <: Expr](expr: T, exprs: T*): RecordHeaderNew = (expr +: exprs).foldLeft(this)(_ withExpr _)

  def withExprs[T <: Expr](exprs: Set[T]): RecordHeaderNew = withExprs(exprs.head, exprs.tail.toSeq: _*)

  def withAlias(exprAsVar: (Expr, Var)*): RecordHeaderNew = exprAsVar.foldLeft(this){
    case (currentHeader, (expr, alias)) => currentHeader.withAlias(expr, alias)
  }

  def withAlias(to: Expr, alias: Var): RecordHeaderNew = {
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

  def --[T <: Expr](expressions: Set[T]): RecordHeaderNew = {
    val expressionToRemove = expressions.flatMap(expressionsFor)
    val updatedExprToColumn = exprToColumn.filterNot { case (e, _) => expressionToRemove.contains(e) }
    copy(exprToColumn = updatedExprToColumn)
  }

  protected def addExprToColumn(expr: Expr, columnName: String): RecordHeaderNew = {
    copy(exprToColumn = exprToColumn + (expr -> columnName))
  }

  def pretty: String = exprToColumn.toSeq.sortBy(_._2).mkString("\n")

}

