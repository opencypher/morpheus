package org.opencypher.spark.api.record

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{Label, PropertyKey}
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.exception.Raise.duplicateEmbeddedEntityColumn

import scala.language.implicitConversions

sealed trait EmbeddedEntity {

  type Self <: EmbeddedEntity

  def entityVar: Var
  def entitySlot: String
  def idSlot: String
  def propertiesFromSlots: Map[String, Set[String]]

  def withProperties(propertyAndSlotNames: Set[String]): Self

  final def withProperty(propertyAndSlotName: String): Self =
    withProperty(propertyAndSlotName -> propertyAndSlotName)

  def withProperty(propertyNameAndSlot: (String, String)): Self

  def slots: Map[String, Expr] = {
    val keyMap = propertiesFromSlots
      .collect { case (key, slots) => slots.map { slot => slot -> Property(entityVar, PropertyKey(key))(CTWildcard) } }
      .flatten
      .foldLeft(Map.empty[String, Expr]) {
        case (m, (slot, expr)) => if (m.contains(slot)) duplicateEmbeddedEntityColumn(slot) else m.updated(slot, expr)
      }

    if (keyMap.contains(idSlot)) duplicateEmbeddedEntityColumn(idSlot) else keyMap.updated(idSlot, entityVar)
  }
}

final case class EmbeddedNode(
  entitySlot: String,
  idSlot: String,
  labelsFromSlotOrImplied: Map[String, Option[String]] = Map.empty,
  propertiesFromSlots: Map[String, Set[String]] = Map.empty
) extends EmbeddedEntity {

  override type Self = EmbeddedNode

  override val entityVar = Var(entitySlot)(CTNode(labelsFromSlotOrImplied.mapValues(_.isEmpty)))

  override def withProperties(propertyAndSlotNames: Set[String]): EmbeddedNode =
    (propertyAndSlotNames -- slots.keySet).foldLeft(this) { case (acc, slot) => acc.withProperty(slot) }

  override def withProperty(property: (String, String)): EmbeddedNode = {
    val (propertyName, propertySlot) = property
    val newPropertySlots = propertiesFromSlots.getOrElse(propertyName, Set.empty) + propertySlot
    copy(propertiesFromSlots = propertiesFromSlots.updated(propertyName, newPropertySlots))
  }

  def withImpliedLabel(impliedLabel: String): EmbeddedNode =
    copy(labelsFromSlotOrImplied = labelsFromSlotOrImplied.updated(impliedLabel, None))

  def withOptionalLabel(optionalLabelAndSlot: String): EmbeddedNode =
    withOptionalLabel(optionalLabelAndSlot -> optionalLabelAndSlot)

  def withOptionalLabel(optionalLabel: (String, String)): EmbeddedNode = {
    val (labelName, slotName) = optionalLabel
    copy(labelsFromSlotOrImplied = labelsFromSlotOrImplied.updated(labelName, Some(slotName)))
  }

  override def slots: Map[String, Expr] =
    labelsFromSlotOrImplied
      .toSeq
      .collect { case (label, Some(slot)) => slot -> HasLabel(entityVar, Label(label))(CTBoolean) }
      .foldLeft(super.slots) {
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
) extends EmbeddedEntity{

  override type Self = EmbeddedRelationship

  override val entityVar = Var(entitySlot)(CTRelationship(relTypeNames))

  def relTypeNames: Set[String] = relTypeSlotOrName match {
    case Left((_, names)) => names
    case Right(name) => Set(name)
  }

  override def withProperties(propertyAndSlotNames: Set[String]): EmbeddedRelationship =
    (propertyAndSlotNames -- slots.keySet).foldLeft(this) { case (acc, slot) => acc.withProperty(slot) }

  override def withProperty(property: (String, String)): EmbeddedRelationship = {
    val (propertyName, propertySlot) = property
    val newPropertySlots = propertiesFromSlots.getOrElse(propertyName, Set.empty) + propertySlot
    copy(propertiesFromSlots = propertiesFromSlots.updated(propertyName, newPropertySlots))
  }

  override def slots: Map[String, Expr] = {
    val slots = Seq(
      fromSlot -> StartNode(entityVar)(CTInteger),
      toSlot -> EndNode(entityVar)(CTInteger)
    )
    .foldLeft(super.slots) { case (acc, (slot, expr)) =>
      if (acc.contains(slot)) duplicateEmbeddedEntityColumn(slot) else acc.updated(slot, expr)
    }
    relTypeSlotOrName match {
      case Left((slot, _)) if slots.contains(slot) => duplicateEmbeddedEntityColumn(slot)
      case Left((slot, _)) => slots.updated(slot, TypeId(entityVar)(CTString))
      case Right(_) => slots
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
  entitySlotAndIdSlot: VIA, fromSlot: FROM, toSlot: TO, relTypeOrSlotName: TYP
) {

  def from(newFromSlot: String) = copy(fromSlot = newFromSlot)

  def as(newEntityAndIdSlot: String) =
    copy(entitySlotAndIdSlot = newEntityAndIdSlot -> newEntityAndIdSlot)

  def as(newEntitySlotAndIdSlot: (String, String)) =
    copy(entitySlotAndIdSlot = newEntitySlotAndIdSlot)

  def relType(newRelTypeName: String) =
    copy(relTypeOrSlotName = Right(newRelTypeName))

  def relTypes(newRelTypeSlot: String, relTypeNames: String*) =
    copy(relTypeOrSlotName = Left(newRelTypeSlot -> relTypeNames.toSet))

  def to(newToSlot: String) =
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
