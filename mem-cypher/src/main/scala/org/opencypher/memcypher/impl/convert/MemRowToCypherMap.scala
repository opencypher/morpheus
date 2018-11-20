package org.opencypher.memcypher.impl.convert

import org.opencypher.memcypher.impl.table.{Row, Schema}
import org.opencypher.okapi.api.types.{CTList, CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.api.value._
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr.{Expr, ListSegment, Var}
import org.opencypher.okapi.relational.impl.table.RecordHeader

final case class MemRowToCypherMap(header: RecordHeader, schema: Schema) extends (Row => CypherMap) {

  override def apply(MemRow: Row): CypherMap = {
    val values = header.returnItems.map(r => r.name -> constructValue(MemRow, r)).toSeq
    CypherMap(values: _*)
  }

  private def constructValue(MemRow: Row, v: Var): CypherValue = {
    v.cypherType.material match {
      case _: CTNode =>
        collectNode(MemRow, v)

      case _: CTRelationship =>
        collectRel(MemRow, v)

      case CTList(_) if !header.exprToColumn.contains(v) =>
        collectComplexList(MemRow, v)

      case _ => constructFromExpression(MemRow, v)
    }
  }

  private def constructFromExpression(row: Row, expr: Expr): CypherValue =
    CypherValue(row.getAs[Any](schema.fieldIndex(header.column(expr))))

  private def collectNode(row: Row, v: Var): CypherValue = {
    val idValue = row.getAs[Any](schema.fieldIndex(header.column(v)))
    idValue match {
      case null => CypherNull
      case id: Long =>

        val labels = header
          .labelsFor(v)
          .map { l => l.label.name -> row.getAs[Boolean](schema.fieldIndex(header.column(l))) }
          .collect { case (name, true) => name }

        val properties = header
          .propertiesFor(v)
          .map { p => p.key.name -> constructFromExpression(row, p) }
          .collect { case (key, value) if !value.isNull => key -> value }
          .toMap

        MemNode(id, labels, properties)
      case invalidID => throw UnsupportedOperationException(s"Node ID has to be a Long instead of ${invalidID.getClass}")
    }
  }

  private def collectRel(row: Row, v: Var): CypherValue = {
    val idValue = row.getAs[Any](schema.fieldIndex(header.column(v)))
    idValue match {
      case null => CypherNull
      case id: Long =>
        val source = row.getAs[Long](schema.fieldIndex(header.column(header.startNodeFor(v))))
        val target = row.getAs[Long](schema.fieldIndex(header.column(header.endNodeFor(v))))

        val relType = header
          .typesFor(v)
          .map { l => l.relType.name -> row.getAs[Boolean](schema.fieldIndex(header.column(l))) }
          .collect { case (name, true) => name }
          .head

        val properties = header
          .propertiesFor(v)
          .map { p => p.key.name -> constructFromExpression(row, p) }
          .collect { case (key, value) if !value.isNull => key -> value }
          .toMap

        MemRelationship(id, source, target, relType, properties)
      case invalidID => throw UnsupportedOperationException(s"Relationship ID has to be a Long instead of ${invalidID.getClass}")
    }
  }

  private def collectComplexList(MemRow: Row, expr: Var): CypherList = {
    val elements = header.ownedBy(expr).collect {
      case p: ListSegment => p
    }.toSeq.sortBy(_.index)

    val values = elements
      .map(constructValue(MemRow, _))
      .filter {
        case CypherNull => false
        case _ => true
      }

    CypherList(values)
  }
}

case class MemNode(
  override val id: Long,
  override val labels: Set[String],
  override val properties: CypherMap
) extends CypherNode[Long] {

  type I = MemNode

  override def copy(
    id: Long = id,
    labels: Set[String] = labels,
    properties: CypherMap = properties
  ): MemNode = MemNode(id, labels, properties)

}

case class MemRelationship(
  override val id: Long,
  override val startId: Long,
  override val endId: Long,
  override val relType: String,
  override val properties: CypherMap
) extends CypherRelationship[Long] {

  type I = MemRelationship

  override def copy(
    id: Long = id,
    source: Long = startId,
    target: Long = endId,
    relType: String = relType,
    properties: CypherMap = properties
  ): MemRelationship = MemRelationship(id, source, target, relType, properties)

}
