/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTBoolean, CTNode, CTString, _}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}

object RecordHeader {

  type RecordSlot = (Expr, SlotContent)

  implicit class RichRecordSlot(val slot: RecordSlot) extends AnyVal {
    def key: Expr = slot._1

    def value: SlotContent = slot._2

    def content: SlotContent = value

    def withOwner(v: Var): RecordSlot = value.withOwner(v).toRecordSlot
  }

  type RecordHeader = Map[Expr, SlotContent]

  val empty: RecordHeader = Map.empty[Expr, SlotContent]

  def apply(contents: SlotContent*): RecordHeader = fromSlotContents(contents)

  def fromSlots(slots: Seq[RecordSlot]): RecordHeader = apply(slots.map(_.content): _*)

  def fromSlots(slots: Set[RecordSlot]): RecordHeader = fromSlots(slots.toSeq)

  def fromSlotContents(contents: Set[SlotContent]): RecordHeader = fromSlotContents(contents.toSeq)

  def fromSlotContents(contents: Seq[SlotContent]): RecordHeader = contents.map(_.toRecordSlot).toMap

  def forNode(node: Var, schema: Schema): RecordHeader = {
    val labels: Set[String] = node.cypherType match {
      case CTNode(l, _) => l
      case other => throw IllegalArgumentException("CTNode", other)
    }
    forNodeWithExplicitLabels(node, schema, labels)
  }

  protected def forNodeWithExplicitLabels(node: Var, schema: Schema, labels: Set[String]): RecordHeader = {
    val labelCombos = if (labels.isEmpty) {
      // all nodes scan
      schema.allLabelCombinations
    } else {
      // label scan
      val impliedLabels = schema.impliedLabels.transitiveImplicationsFor(labels)
      schema.combinationsFor(impliedLabels)
    }

    // create a label column for each possible label
    // optimisation enabled: will not add columns for implied or impossible labels
    val labelExprs = labelCombos.flatten.toSeq.sorted.map { label =>
      ProjectedExpr(HasLabel(node, Label(label))(CTBoolean))
    }

    val propertyKeys = schema.keysFor(labelCombos)
    val propertyExprs = propertyKeys.toSeq.sortBy(_._1).map {
      case (k, t) => ProjectedExpr(Property(node, PropertyKey(k))(t))
    }

    val projectedExprs = labelExprs ++ propertyExprs

    RecordHeader.fromSlotContents(OpaqueField(node) +: projectedExprs)
  }

  def forRelationship(rel: Var, schema: Schema): RecordHeader = {
    val types: Set[String] = rel.cypherType match {
      case CTRelationship(_types, _) if _types.isEmpty =>
        schema.relationshipTypes
      case CTRelationship(_types, _) =>
        _types
      case other =>
        throw IllegalArgumentException("CTRelationship", other)
    }

    forRelationshipWithExplicitType(rel, schema, types)
  }

  protected def forRelationshipWithExplicitType(rel: Var, schema: Schema, relTypes: Set[String]): RecordHeader = {
    val relKeyHeaderProperties = relTypes.toSeq
      .flatMap(t => schema.relationshipKeys(t).toSeq)
      .groupBy(_._1)
      .mapValues { keys =>
        if (keys.size == relTypes.size && keys.forall(keys.head == _)) {
          keys.head._2
        } else {
          keys.head._2.nullable
        }
      }

    val relKeyHeaderContents = relKeyHeaderProperties.toSeq.sortBy(_._1).map {
      case ((k, t)) => ProjectedExpr(Property(rel, PropertyKey(k))(t))
    }

    val startNode = ProjectedExpr(StartNode(rel)(CTNode))
    val typeString = ProjectedExpr(Type(rel)(CTString))
    val endNode = ProjectedExpr(EndNode(rel)(CTNode))

    val relHeaderContents = Seq(startNode, OpaqueField(rel), typeString, endNode) ++ relKeyHeaderContents
    RecordHeader.fromSlotContents(relHeaderContents)
  }

  /**
    * A header for a CypherRecords.
    *
    * The header consists of a number of slots, each of which represents a Cypher expression.
    * The slots that represent variables (which is a kind of expression) are called <i>fields</i>.
    */
  implicit class RichRecordHeader(val header: RecordHeader) extends AnyVal {
    /**
      * The set of record slots stored in this header.
      *
      * @return slots in this header
      */
    def slots: Set[RecordSlot] = header.toSet

    def contents: Set[SlotContent] = header.values.toSet

    def withSlot(slotContent: SlotContent): RecordHeader = header.updated(slotContent.key, slotContent)

    def withSlot(slot: RecordSlot): RecordHeader = withSlot(slot.content)

    def withSlots(slots: RecordSlot*): RecordHeader = header ++ slots

    def withSlotContent(slotContent: SlotContent): RecordHeader = header + slotContent.toRecordSlot

    def withSlotContents(slotContents: SlotContent*): RecordHeader = header ++ slotContents.map(_.toRecordSlot)

    /**
      * The set of fields contained in this header.
      *
      * @return the fields in this header.
      */
    def fields: Set[Var] = header.keySet.collect { case v: Var => v }

    def fieldNames: Set[String] = fields.map(_.name)

    def slotFor(expr: Expr): RecordSlot = expr -> header(expr)

    def slotFor(varName: String): Option[RecordSlot] = {
      fields.collectFirst { case v@Var(name) if name == varName => v }.map(slotFor)
    }

    def sourceNodeSlot(rel: Var): RecordSlot = slotFor(StartNode(rel)())

    def targetNodeSlot(rel: Var): RecordSlot = slotFor(EndNode(rel)())

    def typeSlot(rel: Expr): RecordSlot = slotFor(Type(rel)())

    def labels(node: Var): Seq[HasLabel] = labelSlots(node).keys.toSeq

    def properties(node: Var): Seq[Property] = propertySlots(node).keys.toSeq

    def select(field: Var): RecordHeader = selfWithChildren(field)

    def select(fields: Set[Var]): RecordHeader = fields.flatMap(selfWithChildren).toMap

    protected def selfWithChildren(field: Var): RecordHeader = {
      if (header.contains(field)) {
        childSlots(field) + slotFor(field)
      } else {
        RecordHeader.empty
      }
    }

    def childSlots(entity: Var): RecordHeader = {
      slots.filter {
        case (_, OpaqueField(_)) => false
        case slot if slot.content.owner.contains(entity) => true
        case _ => false
      }.toMap
    }

    def labelSlots(node: Var): Map[HasLabel, RecordSlot] = {
      slots.collect {
        case s@(_, ProjectedExpr(h: HasLabel)) if h.node == node => h -> s
        case s@(_, ProjectedField(_, h: HasLabel)) if h.node == node => h -> s
      }.toMap
    }

    def propertySlots(entity: Var): Map[Property, RecordSlot] = {
      slots.collect {
        case s@(_, ProjectedExpr(p: Property)) if p.m == entity => p -> s
        case s@(_, ProjectedField(_, p: Property)) if p.m == entity => p -> s
      }.toMap
    }

    def nodesForType(nodeType: CTNode): Set[Var] = {
      slots.collect {
        case (_, OpaqueField(v)) => v
      }.filter { v =>
        v.cypherType match {
          case CTNode(labels, _) =>
            val allPossibleLabels = this.labels(v).map(_.label.name).toSet ++ labels
            nodeType.labels.subsetOf(allPossibleLabels)
          case _ => false
        }
      }
    }

    def relationshipsForType(relType: CTRelationship): Set[Var] = {
      val targetTypes = relType.types

      slots.collect {
        case (_, OpaqueField(v)) => v
      }.filter { v =>
        v.cypherType match {
          case t: CTRelationship if targetTypes.isEmpty || t.types.isEmpty => true
          case CTRelationship(types, _) =>
            types.exists(targetTypes.contains)
          case _ => false
        }
      }
    }
  }

}
