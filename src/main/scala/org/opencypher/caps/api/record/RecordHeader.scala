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
package org.opencypher.caps.api.record

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTBoolean, CTNode, CTString, CypherType, _}
import org.opencypher.caps.common.syntax._
import org.opencypher.caps.impl.record.InternalHeader
import org.opencypher.caps.impl.syntax.header.{addContents, _}
import org.opencypher.caps.ir.api.global.TokenRegistry

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

  def sourceNodeSlot(rel: Var): RecordSlot = slotsFor(StartNode(rel)()).headOption.getOrElse(???)
  def targetNodeSlot(rel: Var): RecordSlot = slotsFor(EndNode(rel)()).headOption.getOrElse(???)
  def typeSlot(rel: Expr): RecordSlot = slotsFor(OfType(rel)()).headOption.getOrElse(???)

  def labels(node: Var): Seq[HasLabel] = labelSlots(node).keys.toSeq

  def properties(node: Var): Seq[Property] = propertySlots(node).keys.toSeq

  def childSlots(entity: Var): Seq[RecordSlot] = {
    slots.filter {
      case RecordSlot(_, OpaqueField(_)) => false
      case slot if slot.content.owner.contains(entity) => true
      case _ => false
    }
  }

  def labelSlots(node: Var): Map[HasLabel, RecordSlot] = {
    slots.collect {
      case s@RecordSlot(_, ProjectedExpr(h: HasLabel)) if h.node == node => h -> s
      case s@RecordSlot(_, ProjectedField(_, h: HasLabel)) if h.node == node => h -> s
    }.toMap
  }

  def propertySlots(entity: Var): Map[Property, RecordSlot] = {
    slots.collect {
      case s@RecordSlot(_, ProjectedExpr(p: Property)) if p.m == entity => p -> s
      case s@RecordSlot(_, ProjectedField(_, p: Property)) if p.m == entity => p -> s
    }.toMap
  }

  def nodesForType(nodeType: CTNode): Seq[Var] = {
    slots.collect {
      case RecordSlot(_, OpaqueField(v)) => v
    }.filter { v =>
      v.cypherType match {
        case CTNode(labels) =>
          val allPossibleLabels = this.labels(v).map(_.label.name).toSet ++ labels
          nodeType.labels.subsetOf(allPossibleLabels)
        case _ => false
      }
    }
  }

  def relationshipsForType(relType: CTRelationship): Seq[Var] = {
    val targetTypes = relType.types

    slots.collect {
      case RecordSlot(_, OpaqueField(v)) => v
    }.filter { v =>
      v.cypherType match {
        case t: CTRelationship if targetTypes.isEmpty || t.types.isEmpty => true
        case CTRelationship(types) =>
          types.exists(targetTypes.contains)
        case _ => false
      }
    }
  }

  def rowEncoder: ExpressionEncoder[Row] = {
    val schema = StructType(internalHeader.slots.map(_.structField))
    RowEncoder(schema)
  }

  override def toString: String = {
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
    val headerLabels = impliedLabels ++ possibleLabels
    val labelHeaderContents = headerLabels.map {
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
    val typeString = ProjectedExpr(OfType(rel)(CTString))
    val endNode = ProjectedExpr(EndNode(rel)(CTNode))

    val relHeaderContents = Seq(startNode, OpaqueField(rel), typeString, endNode) ++ relKeyHeaderContents
    // this header is necessary on its own to get the type filtering right
    val (relHeader, _) = RecordHeader.empty.update(addContents(relHeaderContents))

    relHeader
  }
}
