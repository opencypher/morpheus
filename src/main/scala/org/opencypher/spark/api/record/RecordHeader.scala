package org.opencypher.spark.api.record

import cats.Monoid
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.GlobalsRegistry
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

  def slotFor(variable: Var): RecordSlot = slotsFor(variable).headOption.getOrElse(???)
  def slotsFor(names: String*): Seq[RecordSlot] =
    names.map(n => internalHeader.slotsByName(n).headOption.getOrElse(???))

  def mandatory(slot: RecordSlot): Boolean =
    internalHeader.mandatory(slot)

  def sourceNode(rel: Var): RecordSlot = slotsFor(StartNode(rel)()).headOption.getOrElse(???)
  def targetNode(rel: Var): RecordSlot = slotsFor(EndNode(rel)()).headOption.getOrElse(???)
  def typeId(rel: Expr): RecordSlot = slotsFor(TypeId(rel)()).headOption.getOrElse(???)

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
  private implicit val typeVectorMonoid = new Monoid[Vector[CypherType]] {
    override def empty: Vector[CypherType] = Vector.empty
    override def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType] = x ++ y
  }

  def nodeFromSchema(node: Var, schema: Schema, globals: GlobalsRegistry): RecordHeader =
    nodeFromSchema(node, schema, globals, schema.labels)

  def nodeFromSchema(node: Var, schema: Schema, globals: GlobalsRegistry, labels: Set[String]): RecordHeader = {
    val impliedLabels = schema.impliedLabels.transitiveImplicationsFor(schema.labels)
    val impliedKeys = impliedLabels.flatMap(label => schema.nodeKeyMap.keysFor(label).toSet)
    val possibleLabels = impliedLabels.flatMap(label => schema.optionalLabels.combinationsFor(label))
    val optionalKeys = possibleLabels.flatMap(label => schema.nodeKeyMap.keysFor(label).toSet)
    val optionalNullableKeys = optionalKeys.map { case (k, v) => k -> v.nullable }
    val allKeys = (impliedKeys ++ optionalNullableKeys).toSeq.map { case (k, v) => k -> Vector(v) }
    val keyGroups = allKeys.groups[String, Vector[CypherType]]

    val labelHeaderContents = (impliedLabels ++ possibleLabels).map {
      labelName => ProjectedExpr(HasLabel(node, globals.labelByName(labelName))(CTBoolean))
    }.toSeq

    // TODO: This should consider multiple types per property
    val keyHeaderContents = keyGroups.toSeq.flatMap {
      case (k, types) => types.map { t => ProjectedExpr(Property(node, globals.propertyKeyByName(k))(t)) }
    }

    // TODO: Add is null column(?)

    // TODO: Check results for errors
    val (header, _) = RecordHeader.empty
      .update(addContents(OpaqueField(node) +: (labelHeaderContents ++ keyHeaderContents)))

    header
  }

  def relationshipFromSchema(rel: Var, schema: Schema, globals: GlobalsRegistry): RecordHeader =
    relationshipFromSchema(rel, schema, globals, schema.relationshipTypes)

  def relationshipFromSchema(rel: Var, schema: Schema, globals: GlobalsRegistry, relTypes: Set[String]): RecordHeader = {
    val relKeyHeaderProperties = relTypes.flatMap(t => schema.relationshipKeys(t).toSeq)

    val relKeyHeaderContents = relKeyHeaderProperties.map {
      case ((k, t)) => ProjectedExpr(Property(rel, globals.propertyKeyByName(k))(t))
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
