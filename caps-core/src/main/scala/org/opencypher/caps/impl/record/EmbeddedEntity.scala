/*
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
package org.opencypher.caps.impl.record

import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.types._
import org.opencypher.caps.common.{Verifiable, Verified}
import org.opencypher.caps.impl.exception.Raise.duplicateEmbeddedEntityColumn
import org.opencypher.caps.ir.api.{Label, PropertyKey}

sealed trait EmbeddedEntity extends Verifiable {

  override type Self <: EmbeddedEntity
  override type VerifiedSelf = VerifiedEmbeddedEntity[Self]

  type EntityCypherType <: CypherType

  // This method is not implemented here to avoid initialization order issues
  def entityVar: Var
  def entityType: EntityCypherType

  def entitySlot: String
  def idSlot: String
  def propertiesFromSlots: Map[String, Set[String]]

  def withProperties(propertyAndSlotNames: Set[String]): Self

  final def withProperty(propertyAndSlotName: String): Self =
    withPropertyKey(propertyAndSlotName -> propertyAndSlotName)

  def withPropertyKey(propertyNameAndSlot: (String, String)): Self

  def withPropertyKey(property: String): Self =
    withPropertyKey(property -> property)

  def withPropertyKeys(properties: String*): Self

  protected def computeSlots: Map[String, Expr] = {
    val keyMap = propertiesFromSlots.collect {
      case (key, slots) =>
        slots.map { slot =>
          slot -> Property(entityVar, PropertyKey(key))()
        }
    }.flatten
      .foldLeft(Map.empty[String, Expr]) {
        case (m, (slot, expr)) => if (m.contains(slot)) duplicateEmbeddedEntityColumn(slot) else m.updated(slot, expr)
      }

    if (keyMap.contains(idSlot)) duplicateEmbeddedEntityColumn(idSlot) else keyMap.updated(idSlot, entityVar)
  }
}

sealed trait VerifiedEmbeddedEntity[V <: EmbeddedEntity] extends Verified[V] {
  def slots: Map[String, Expr]
}

final case class EmbeddedNode(
    entitySlot: String,
    idSlot: String,
    labelsFromSlotOrImplied: Map[String, Option[String]] = Map.empty,
    propertiesFromSlots: Map[String, Set[String]] = Map.empty
) extends EmbeddedEntity {

  self =>

  override type Self = EmbeddedNode
  override type EntityCypherType = CTNode

  override val entityType = CTNode(labelsFromSlotOrImplied.collect { case (label, None) => label }.toSet)
  override val entityVar = Var(entitySlot)(entityType)

  override def verify: VerifiedSelf = new VerifiedEmbeddedEntity[EmbeddedNode] {
    override val v: EmbeddedNode = self
    override val slots: Map[String, Expr] = computeSlots
  }

  override def withProperties(propertyAndSlotNames: Set[String]): EmbeddedNode =
    (propertyAndSlotNames -- computeSlots.keySet).foldLeft(this) { case (acc, slot) => acc.withProperty(slot) }

  override def withPropertyKey(property: (String, String)): EmbeddedNode = {
    val (propertyName, propertySlot) = property
    val newPropertySlots = propertiesFromSlots.getOrElse(propertyName, Set.empty) + propertySlot
    copy(propertiesFromSlots = propertiesFromSlots.updated(propertyName, newPropertySlots))
  }

  override def withPropertyKeys(properties: String*): EmbeddedNode =
    properties.foldLeft(this)((entity, property) => entity.withPropertyKey(property))

  def withImpliedLabel(impliedLabel: String): EmbeddedNode =
    copy(labelsFromSlotOrImplied = labelsFromSlotOrImplied.updated(impliedLabel, None))

  def withImpliedLabels(impliedLabels: String*): EmbeddedNode =
    impliedLabels.foldLeft(this)((node, label) => node.withImpliedLabel(label))

  def withOptionalLabel(optionalLabelAndSlot: String): EmbeddedNode =
    withOptionalLabel(optionalLabelAndSlot -> optionalLabelAndSlot)

  def withOptionalLabel(optionalLabel: (String, String)): EmbeddedNode = {
    val (labelName, slotName) = optionalLabel
    copy(labelsFromSlotOrImplied = labelsFromSlotOrImplied.updated(labelName, Some(slotName)))
  }

  override protected def computeSlots: Map[String, Expr] =
    labelsFromSlotOrImplied.toSeq.collect {
      case (label, Some(slot)) => slot -> HasLabel(entityVar, Label(label))(CTBoolean)
    }.foldLeft(super.computeSlots) {
      case (m, (slot, expr)) => if (m.contains(slot)) duplicateEmbeddedEntityColumn(slot) else m.updated(slot, expr)
    }
}

object EmbeddedNode extends EmbeddedNodeBuilder(()) {

  def apply(entityAndIdSlot: String): EmbeddedNodeBuilder[(String, String)] =
    apply(entityAndIdSlot -> entityAndIdSlot)

  def apply(entitySlotAndIdSlot: (String, String)): EmbeddedNodeBuilder[(String, String)] =
    EmbeddedNodeBuilder(entitySlotAndIdSlot)
}

sealed case class EmbeddedNodeBuilder[VIA](entitySlotAndIdSlot: VIA) {

  def as(newEntityAndIdSlot: String) =
    copy(entitySlotAndIdSlot = newEntityAndIdSlot -> newEntityAndIdSlot)

  def as(newEntitySlotAndIdSlot: (String, String)) =
    copy(entitySlotAndIdSlot = newEntitySlotAndIdSlot)
}

object EmbeddedNodeBuilder {
  implicit final class RichBuilder(val builder: EmbeddedNodeBuilder[(String, String)]) extends AnyVal {
    def build: EmbeddedNode =
      EmbeddedNode(
        builder.entitySlotAndIdSlot._1,
        builder.entitySlotAndIdSlot._2
      )
  }
}

final case class EmbeddedRelationship(
    entitySlot: String,
    idSlot: String,
    fromSlot: String,
    relTypeSlotOrName: Either[(String, Set[String]), String],
    toSlot: String,
    propertiesFromSlots: Map[String, Set[String]] = Map.empty
) extends EmbeddedEntity {

  self =>

  override type Self = EmbeddedRelationship
  override type EntityCypherType = CTRelationship

  override val entityType = CTRelationship(relTypeNames)
  override val entityVar = Var(entitySlot)(entityType)

  override def verify: VerifiedSelf = new VerifiedEmbeddedEntity[EmbeddedRelationship] {
    override val v: EmbeddedRelationship = self
    override val slots: Map[String, Expr] = computeSlots
  }

  def relTypeNames: Set[String] = relTypeSlotOrName match {
    case Left((_, names)) => names
    case Right(name)      => Set(name)
  }

  override def withProperties(propertyAndSlotNames: Set[String]): EmbeddedRelationship =
    (propertyAndSlotNames -- computeSlots.keySet).foldLeft(this) { case (acc, slot) => acc.withProperty(slot) }

  override def withPropertyKey(property: (String, String)): EmbeddedRelationship = {
    val (propertyName, propertySlot) = property
    val newPropertySlots = propertiesFromSlots.getOrElse(propertyName, Set.empty) + propertySlot
    copy(propertiesFromSlots = propertiesFromSlots.updated(propertyName, newPropertySlots))
  }

  override def withPropertyKeys(properties: String*): EmbeddedRelationship =
    properties.foldLeft(this)((entity, property) => entity.withPropertyKey(property))

  override protected def computeSlots: Map[String, Expr] = {
    val slots = Seq(
      fromSlot -> StartNode(entityVar)(CTInteger),
      toSlot -> EndNode(entityVar)(CTInteger)
    ).foldLeft(super.computeSlots) {
      case (acc, (slot, expr)) =>
        if (acc.contains(slot)) duplicateEmbeddedEntityColumn(slot) else acc.updated(slot, expr)
    }
    relTypeSlotOrName match {
      case Left((slot, _)) if slots.contains(slot) => duplicateEmbeddedEntityColumn(slot)
      case Left((slot, _))                         => slots.updated(slot, Type(entityVar)(CTString))
      case Right(_)                                => slots
    }
  }
}

object EmbeddedRelationship extends EmbeddedRelationshipBuilder((), (), (), ()) {

  def apply(entityAndIdSlot: String): EmbeddedRelationshipBuilder[Unit, (String, String), Unit, Unit] =
    apply(entityAndIdSlot -> entityAndIdSlot)

  def apply(entitySlotAndIdSlot: (String, String)): EmbeddedRelationshipBuilder[Unit, (String, String), Unit, Unit] =
    EmbeddedRelationshipBuilder(entitySlotAndIdSlot, (), (), ())
}

sealed case class EmbeddedRelationshipBuilder[FROM, VIA, TYP, TO](
    entitySlotAndIdSlot: VIA,
    fromSlot: FROM,
    toSlot: TO,
    relTypeOrSlotName: TYP
) {

  def from(newFromSlot: String): EmbeddedRelationshipBuilder[String, VIA, TYP, TO] = copy(fromSlot = newFromSlot)

  def as(newEntityAndIdSlot: String): EmbeddedRelationshipBuilder[FROM, (String, String), TYP, TO] =
    copy(entitySlotAndIdSlot = newEntityAndIdSlot -> newEntityAndIdSlot)

  def as(newEntitySlotAndIdSlot: (String, String)): EmbeddedRelationshipBuilder[FROM, (String, String), TYP, TO] =
    copy(entitySlotAndIdSlot = newEntitySlotAndIdSlot)

  def relType(newRelTypeName: String): EmbeddedRelationshipBuilder[FROM, VIA, Right[Nothing, String], TO] =
    copy(relTypeOrSlotName = Right(newRelTypeName))

  def relTypes(
      newRelTypeSlot: String,
      relTypeNames: String*): EmbeddedRelationshipBuilder[FROM, VIA, Left[(String, Set[String]), Nothing], TO] =
    copy(relTypeOrSlotName = Left(newRelTypeSlot -> relTypeNames.toSet))

  def to(newToSlot: String): EmbeddedRelationshipBuilder[FROM, VIA, TYP, String] =
    copy(toSlot = newToSlot)
}

object EmbeddedRelationshipBuilder {

  implicit final class RichBuilder[TYP <: Either[(String, Set[String]), String]](
      val builder: EmbeddedRelationshipBuilder[String, (String, String), TYP, String]
  ) extends AnyVal {
    def build: EmbeddedRelationship =
      EmbeddedRelationship(
        builder.entitySlotAndIdSlot._1,
        builder.entitySlotAndIdSlot._2,
        builder.fromSlot,
        builder.relTypeOrSlotName,
        builder.toSlot
      )
  }
}
