/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.spark.api.record

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTBoolean, CTInteger, CTNode, CypherType}
import org.opencypher.spark.impl.record.InternalHeader
import org.opencypher.spark.impl.syntax.header.{addContents, _}
import org.opencypher.spark.impl.syntax.util.traversable._

final case class RecordHeader(internalHeader: InternalHeader) {

  def ++(other: RecordHeader): RecordHeader =
    copy(internalHeader ++ other.internalHeader)

  def indexOf(content: SlotContent): Option[Int] = slots.find(_.content == content).map(_.index)
  def slots: IndexedSeq[RecordSlot] = internalHeader.slots
  def contents: Set[SlotContent] = slots.map(_.content).toSet
  def fields: Set[Var] = internalHeader.fields

  def slotsFor(expr: Expr): Seq[RecordSlot] =
    internalHeader.slotsFor(expr)

  // TODO: Push error handling to API consumers

  def slotFor(variable: Var): RecordSlot =
    slotsFor(variable).headOption.getOrElse(???)

  def mandatory(slot: RecordSlot): Boolean =
    internalHeader.mandatory(slot)

  def sourceNode(rel: Var): RecordSlot = slotsFor(StartNode(rel)()).headOption.getOrElse(???)
  def targetNode(rel: Var): RecordSlot = slotsFor(EndNode(rel)()).headOption.getOrElse(???)
  def typeId(rel: Expr): RecordSlot = slotsFor(TypeId(rel)()).headOption.getOrElse(???)

  def labels(node: Var): Seq[HasLabel] = {
    val slotsForNode = slots.filter(_.content.owner.orNull == node)
    slotsForNode.collect({
      case RecordSlot(_, ProjectedExpr(h: HasLabel)) => h
    })
  }

  def childSlots(node: Var): Set[RecordSlot] = {
    slots.filter {
      case RecordSlot(_, OpaqueField(_)) => false
      case slot if slot.content.owner.orNull == node => true
      case _ => false
    }.toSet
  }

  override def toString = {
    val s = slots
    s"RecordHeader with ${s.size} slots: \n\t ${slots.mkString("\n\t")}"
  }
}

object RecordHeader {

  def empty: RecordHeader =
    RecordHeader(InternalHeader.empty)

  def from(contents: SlotContent*): RecordHeader =
    RecordHeader(contents.foldLeft(InternalHeader.empty) { case (header, slot) => header + slot })

  // TODO: Probably move this to an implicit class RichSchema?
  def nodeFromSchema(node: Var, schema: Schema, tokens: TokenRegistry): RecordHeader =
    nodeFromSchema(node, schema, tokens, schema.labels)

  def nodeFromSchema(node: Var, schema: Schema, tokens: TokenRegistry, labels: Set[String]): RecordHeader = {
    val impliedLabels = schema.impliedLabels.transitiveImplicationsFor(if (labels.nonEmpty) labels else schema.labels)
    val impliedKeys = impliedLabels.flatMap(label => schema.nodeKeyMap.keysFor(label).toSet)
    val possibleLabels = impliedLabels.flatMap(label => schema.labelCombinations.combinationsFor(label))
    val optionalKeys = possibleLabels.flatMap(label => schema.nodeKeyMap.keysFor(label).toSet) -- impliedKeys
    val optionalNullableKeys = optionalKeys.map { case (k, v) => k -> v.nullable }
    val allKeys: Seq[(String, Vector[CypherType])] = (impliedKeys ++ optionalNullableKeys).toSeq.map { case (k, v) => k -> Vector(v) }
    val keyGroups: Map[String, Vector[CypherType]] = allKeys.groups[String, Vector[CypherType]]

    val labelHeaderContents = (impliedLabels ++ possibleLabels).map {
      labelName => ProjectedExpr(HasLabel(node, tokens.labelByName(labelName))(CTBoolean))
    }.toSeq

    // TODO: This should consider multiple types per property
    val keyHeaderContents = keyGroups.toSeq.flatMap {
      case (k, types) => types.map { t => ProjectedExpr(Property(node, tokens.propertyKeyByName(k))(t)) }
    }

    // TODO: Add is null column(?)

    // TODO: Check results for errors
    val (header, _) = RecordHeader.empty
      .update(addContents(OpaqueField(node) +: (labelHeaderContents ++ keyHeaderContents)))

    header
  }

  def relationshipFromSchema(rel: Var, schema: Schema, tokens: TokenRegistry): RecordHeader =
    relationshipFromSchema(rel, schema, tokens, schema.relationshipTypes)

  def relationshipFromSchema(rel: Var, schema: Schema, tokens: TokenRegistry, relTypes: Set[String]): RecordHeader = {
    val relKeyHeaderProperties = relTypes.flatMap(t => schema.relationshipKeys(t).toSeq)

    val relKeyHeaderContents = relKeyHeaderProperties.map {
      case ((k, t)) => ProjectedExpr(Property(rel, tokens.propertyKeyByName(k))(t))
    }

    val startNode = ProjectedExpr(StartNode(rel)(CTNode))
    val typeIdContent = ProjectedExpr(TypeId(rel)(CTInteger))
    val endNode = ProjectedExpr(EndNode(rel)(CTNode))

    val relHeaderContents = Seq(startNode, OpaqueField(rel), typeIdContent, endNode) ++ relKeyHeaderContents
    // this header is necessary on its own to get the type filtering right
    val (relHeader, _) = RecordHeader.empty.update(addContents(relHeaderContents))

    relHeader
  }
}
