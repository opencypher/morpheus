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

import cats.instances.all._
import cats.syntax.semigroup._
import org.opencypher.okapi.api.types.{CTNode, _}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader2.ExpressionMapping
import org.opencypher.okapi.relational.impl.util.StringEncodingUtilities._

import scala.util.Random

object RecordHeader2 {

  type ExpressionMapping = (Expr, Set[Var])

  implicit class RichExpressionMapping(val mapping: ExpressionMapping) extends AnyVal {

    def expr: Expr = mapping._1

    def key: Expr = expr

    def fields: Set[Var] = mapping._2

    def value: Set[Var] = fields
  }
}

case class RecordHeader2(header: Map[Expr, Set[Var]]) extends IRecordHeader {
  /**
    * The set of record slots stored in this header.
    *
    * @return slots in this header
    */
  def mappings: Set[ExpressionMapping] = header.toSet

  def mappingFor(expr: Expr): ExpressionMapping = expr -> header(expr)

  def expressions: Set[Expr] = header.keySet

  def withMapping(m: ExpressionMapping): RecordHeader2 = copy(header |+| Map(m))

  def withMappings(m: ExpressionMapping*): RecordHeader2 = copy(header |+| m.toMap)

  def withField(v: Var): RecordHeader2 = {
    // Ensure a field with the same name and a different type is replaced
    val maybeExistingFieldWithSameName = field(v.name)
    maybeExistingFieldWithSameName match {
      case None =>
        // Add new field
        copy(header |+| Map(v -> Set(v)))
      case Some(existing) =>
        if (existing.cypherType == v.cypherType) {
          // Nothing to do, field exists already
          this
        } else {
          // Replace existing field (which could be an alias)
          val (existingKey, oldFields) = exprFor(existing)
          val updatedFields = oldFields - existing + v
          copy(header.updated(existingKey, updatedFields))
        }
    }
  }

  def field(name: String): Option[Var] = fieldsAsVar.find(_.name == name)

  def withFields(vs: Var*): RecordHeader2 = vs.foldLeft(this)(_.withField(_))


  def withExpression(expression: Expr): RecordHeader2 = {
    expression match {
      case v: Var => withField(v)
      case _ => copy(header |+| Map(expression -> Set.empty))
    }
  }

  def withExpressions(expressions: Expr*): RecordHeader2 = {
    expressions.foldLeft(this)(_.withExpression(_))
  }

  //  def selectField(field: Var): RecordHeader2 = selectFields(Set(field))

  /**
    * Returns the header only for the selected fields.
    */
  //  def selectFields(fields: Set[Var]): RecordHeader2 = {
  //    header.flatMap {
  //      case (expr, fs) if fs.exists(fields.contains) || expr.owner.exists(fields.contains) =>
  //        Some(expr -> fs.intersect(fields))
  //      case _ => None
  //    }
  //  }

  override def fields: Set[String] = fieldsAsVar.map(_.name)

  override def fieldsAsVar: Set[Var] = header.values.flatten.toSet

  def getExprFor(expr: Expr): Option[ExpressionMapping] = {
    if (header.contains(expr)) {
      Some(expr -> header(expr))
    } else {
      expr match {
        case v: Var =>
          // Could be an alias
          header.collectFirst {
            case (maybeAliasedExpr, fields) if fields.contains(v) => maybeAliasedExpr -> fields
          }
        case _ => None
      }
    }
  }

  def exprFor(expr: Expr): ExpressionMapping = {
    getExprFor(expr).getOrElse(
      throw IllegalArgumentException(s"""Expr $expr has no associated expression or alias in header ${header}"""))
  }

  def exprFor(fieldName: String): Option[ExpressionMapping] = {
    fieldsAsVar.collectFirst { case v@Var(name) if name == fieldName => v }.map(exprFor)
  }

  def sourceNodeMapping(rel: Var): ExpressionMapping = exprFor(StartNode(rel)())

  def targetNodeMapping(rel: Var): ExpressionMapping = exprFor(EndNode(rel)())

  def typeMapping(rel: Expr): ExpressionMapping = exprFor(Type(rel)())

  def labels(node: Var): Seq[HasLabel] = labelExprs(node).keys.toSeq

  def properties(node: Var): Seq[Property] = propertyExprs(node).keys.toSeq

  //  def ownedExprs(entity: Expr): RecordHeader2 = {
  //    val exprMapping = exprFor(entity)
  //    val entityAliases = exprMapping._2
  //    header.filter {
  //      case mapping if mapping.expr.owner.exists(entityAliases.contains) => true
  //      case _ => false
  //    }
  //  }

  def labelExprs(node: Var): Map[HasLabel, Set[Var]] = {
    mappings.collect {
      case (label@HasLabel(n, _), fields) if node == n => label -> fields
    }.toMap
  }

  def propertyExprs(entity: Var): Map[Property, Set[Var]] = {
    mappings.collect {
      case (property@Property(e, _), fields) if entity == e => property -> fields
    }.toMap
  }

  override def nodesForType(nodeType: CTNode): Seq[Var] = {
    fieldsAsVar.filter { field =>
      field.cypherType match {
        case CTNode(labels, _) =>
          val allPossibleLabels = this.labels(field).map(_.label.name).toSet ++ labels
          nodeType.labels.subsetOf(allPossibleLabels)
        case _ => false
      }
    }.toSeq
  }

  override def relationshipsForType(relType: CTRelationship): Seq[Var] = {
    val targetTypes = relType.types

    fieldsAsVar.filter { field =>
      field.cypherType match {
        case t: CTRelationship if targetTypes.isEmpty || t.types.isEmpty => true
        case CTRelationship(types, _) => types.exists(targetTypes.contains)
        case _ => false
      }
    }.toSeq
  }
  override val columns: Seq[String] = expressions.map(ColumnNamer.of).toSeq
  override def of(slot: RecordSlot): String = of(slot.content)
  override def of(slot: SlotContent): String = of(slot.key)
  override def of(expr: Expr): String = ColumnNamer.of(expr)
  override def pretty: String = toString

  final override def generateUniqueName: String = {
    val NAME_SIZE = 5

    val chars = (1 to NAME_SIZE).map(_ => Random.nextPrintableChar())
    val name = String.valueOf(chars.toArray).encodeSpecialCharacters

    if (slots.map(of).contains(name)) generateUniqueName
    else name
  }
  override def tempColName: String = ColumnNamer.tempColName
  override def column(slot: RecordSlot): String = of(slot)
  override def ++(other: IRecordHeader): IRecordHeader =
    withMappings(other.asInstanceOf[RecordHeader2].header.toSeq: _*)
  override def -(toRemove: RecordSlot): IRecordHeader = copy(header - toRemove.content.key)
  override def --(other: IRecordHeader): IRecordHeader = other.slots.foldLeft(this: IRecordHeader)(_ - _)

  override def slots: IndexedSeq[RecordSlot] = ???
  override def contents: Seq[SlotContent] = ???
  override def fieldsInOrder: Seq[String] = ???
  override def slotsFor(expr: Expr): Seq[RecordSlot] = ???
  override def slotFor(variable: Var): RecordSlot = ???
  override def mandatory(slot: RecordSlot): Boolean = ???
  override def sourceNodeSlot(rel: Var): RecordSlot = ???
  override def targetNodeSlot(rel: Var): RecordSlot = ???
  override def typeSlot(rel: Expr): RecordSlot = ???
  override def select(fields: Set[Var]): IRecordHeader = ???
  override def selfWithChildren(field: Var): Seq[RecordSlot] = ???
  override def childSlots(entity: Var): Seq[RecordSlot] = ???
  override def labelSlots(node: Var): Map[HasLabel, RecordSlot] = ???
  override def propertySlots(entity: Var): Map[Property, RecordSlot] = ???


  override def addContents(contents: Seq[SlotContent]): IRecordHeader = contents.foldLeft(this: IRecordHeader)(_ addContent _)
  override def addContent(content: SlotContent): IRecordHeader = copy(header + (content.key -> content.owner.toSet))
  override def contains(content: SlotContent): Boolean = ???
}
