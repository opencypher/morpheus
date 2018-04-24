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

import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._

final case class RecordSlot(index: Int, content: SlotContent) {
  def withOwner(v: Var): RecordSlot = copy(content = content.withOwner(v))
}

object RecordSlot {
  def from(pair: (Int, SlotContent)) = RecordSlot(pair._1, pair._2)
}

sealed trait SlotContent {

  def key: Expr

  def alias: Option[Var]
  def owner: Option[Var]

  final def cypherType: CypherType = key.cypherType
  def support: Traversable[Expr]

  def withOwner(newOwner: Var): SlotContent
}

sealed trait ProjectedSlotContent extends SlotContent {

  def expr: Expr

  override def owner = expr match {
    case Property(v: Var, _) => Some(v)
    case HasLabel(v: Var, _) => Some(v)
    case HasType(v: Var, _)  => Some(v)
    case StartNode(v: Var)   => Some(v)
    case EndNode(v: Var)     => Some(v)
    case Type(v: Var)        => Some(v)
    case _                   => None
  }
}

sealed trait FieldSlotContent extends SlotContent {
  override def alias = Some(field)
  override def key: Var = field
  def field: Var
}

final case class ProjectedExpr(expr: Expr) extends ProjectedSlotContent {
  override def key = expr
  override def alias = None

  override def support = Seq(expr)

  override def withOwner(newOwner: Var): ProjectedExpr = key match {
    case h: HasLabel  => ProjectedExpr(HasLabel(newOwner, h.label)(h.cypherType))
    case t: Type      => ProjectedExpr(Type(newOwner)(t.cypherType))
    case p: Property  => ProjectedExpr(Property(newOwner, p.key)(p.cypherType))
    case s: StartNode => ProjectedExpr(StartNode(newOwner)(s.cypherType))
    case e: EndNode   => ProjectedExpr(EndNode(newOwner)(e.cypherType))
    case _            => this
  }
}

final case class OpaqueField(field: Var) extends FieldSlotContent {
  override def owner = Some(field)

  override def support = Seq(field)

  override def withOwner(newOwner: Var): SlotContent = copy(Var(newOwner.name)(cypherType))
}

final case class ProjectedField(field: Var, expr: Expr) extends ProjectedSlotContent with FieldSlotContent {

  override def support = Seq(field, expr)

  // TODO: Consider whether withOwner on ProjectedField should return ProjectedExpr
  override def withOwner(newOwner: Var): ProjectedExpr = expr match {
    case h: HasLabel  => ProjectedExpr(HasLabel(newOwner, h.label)(h.cypherType))
    case t: Type      => ProjectedExpr(Type(newOwner)(t.cypherType))
    case p: Property  => ProjectedExpr(Property(newOwner, p.key)(p.cypherType))
    case s: StartNode => ProjectedExpr(StartNode(newOwner)(s.cypherType))
    case e: EndNode   => ProjectedExpr(EndNode(newOwner)(e.cypherType))
    case _            => throw IllegalArgumentException("expression with owner", expr)
  }
}
