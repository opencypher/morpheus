package org.opencypher.okapi.relational.impl.table
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTBoolean, CTNode, CTRelationship, CTString}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.ir.api.expr._

object IRecordHeader {

  def empty: IRecordHeader =
    RecordHeader(InternalHeader.empty)

  def from(slots: List[RecordSlot]): IRecordHeader =
    from(slots.map(_.content): _*)

  def from(contents: SlotContent*): IRecordHeader =
    RecordHeader(contents.foldLeft(InternalHeader.empty) { case (header, slot) => header + slot })

  // TODO: Probably move this to an implicit class RichSchema?
  def nodeFromSchema(node: Var, schema: Schema): IRecordHeader = {
    val labels: Set[String] = node.cypherType match {
      case CTNode(l, _) => l
      case other     => throw IllegalArgumentException("CTNode", other.asInstanceOf[IRecordHeader])
    }
    nodeFromSchema(node, schema, labels)
  }

  def nodeFromSchema(node: Var, schema: Schema, labels: Set[String]): IRecordHeader = {

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
    val header = IRecordHeader.empty.addContents(OpaqueField(node) +: projectedExprs)

    header
  }

  def relationshipFromSchema(rel: Var, schema: Schema): IRecordHeader = {
    val types: Set[String] = rel.cypherType match {
      case CTRelationship(_types, _) if _types.isEmpty =>
        schema.relationshipTypes
      case CTRelationship(_types, _) =>
        _types
      case other =>
        throw IllegalArgumentException("CTRelationship", other.asInstanceOf[IRecordHeader])
    }

    relationshipFromSchema(rel, schema, types)
  }

  def relationshipFromSchema(rel: Var, schema: Schema, relTypes: Set[String]): IRecordHeader = {
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
    val relHeader = IRecordHeader.empty.addContents(relHeaderContents)

    relHeader
  }
}

trait IRecordHeader {

  def addContents(contents: Seq[SlotContent]): IRecordHeader

  def addContent(content: SlotContent): IRecordHeader

  def generateUniqueName: String

  def tempColName: String

  def of(slot: RecordSlot): String

  def of(slot: SlotContent): String

  def of(expr: Expr): String

  val columns: Seq[String]

  def column(slot: RecordSlot): String

  def ++(other: IRecordHeader): IRecordHeader

  def -(toRemove: RecordSlot): IRecordHeader

  def --(other: IRecordHeader): IRecordHeader

  def slots: IndexedSeq[RecordSlot]

  def contains(slot: SlotContent): Boolean

  def contents: Seq[SlotContent]

  def fields: Set[String]

  def fieldsAsVar: Set[Var]

  def fieldsInOrder: Seq[String]

  def slotsFor(expr: Expr): Seq[RecordSlot]

  def slotFor(variable: Var): RecordSlot

  def mandatory(slot: RecordSlot): Boolean

  def sourceNodeSlot(rel: Var): RecordSlot

  def targetNodeSlot(rel: Var): RecordSlot

  def typeSlot(rel: Expr): RecordSlot

  def labels(node: Var): Seq[HasLabel]

  def properties(node: Var): Seq[Property]

  def select(fields: Set[Var]): IRecordHeader

  def selfWithChildren(field: Var): Seq[RecordSlot]

  def childSlots(entity: Var): Seq[RecordSlot]

  def labelSlots(node: Var): Map[HasLabel, RecordSlot]

  def propertySlots(entity: Var): Map[Property, RecordSlot]

  def nodesForType(nodeType: CTNode): Seq[Var]

  def relationshipsForType(relType: CTRelationship): Seq[Var]

  def toString: String

  def pretty: String
}
