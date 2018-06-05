package org.opencypher.okapi.relational.api.io

import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.{CypherRecords, CypherTable}
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTNode, CypherType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.api.io.RelationalEntityMapping._
import org.opencypher.okapi.relational.impl.physical.JoinType
import org.opencypher.okapi.relational.impl.table.RecordHeaderNew

trait FlatRelationalTable[T <: FlatRelationalTable[T]] extends CypherTable {

  def select(cols: String*): T

  def unionAll(other: T): T

  def distinct: T

  def distinct(cols: String*): T

  def withNullColumn(col: String): T

  def withTrueColumn(col: String): T

  def withFalseColumn(col: String): T

  def join(other: T, joinType: JoinType, joinCols: (String, String)*): T

}

trait RelationalCypherRecords[T <: FlatRelationalTable[T]] extends CypherRecords {

  def from(header: RecordHeaderNew, table: T): RelationalCypherRecords[T]

  def table: T

  override def columns: Seq[String] = table.columns

  def header: RecordHeaderNew

  def select(exprs: Expr*): RelationalCypherRecords[T] = {
    val selectHeader = header.select(exprs: _*)
    val selectedColumns = selectHeader.columns
    val selectedData = table.select(selectedColumns.toSeq.sorted: _*)
    from(selectHeader, selectedData)
  }

  def withAliases(originalToAlias: (Expr, Var)*): RelationalCypherRecords[T] = {
    val headerWithAliases = header.withAlias(originalToAlias: _*)
    from(headerWithAliases, table)
  }

  def removeVars(vars: Set[Var]): RelationalCypherRecords[T] = {
    val updatedHeader = header -- vars
    val keepColumns = updatedHeader.columns.toSeq.sorted
    val updatedData = table.select(keepColumns: _*)
    from(updatedHeader, updatedData)
  }

  def unionAll(other: RelationalCypherRecords[T]): RelationalCypherRecords[T] = {
    val unionData = table.unionAll(other.table)
    from(header, unionData)
  }

  def distinct: RelationalCypherRecords[T] = {
    from(header, table.distinct)
  }

  def distinct(fields: Var*): RelationalCypherRecords[T] = {
    from(header, table.distinct(fields.flatMap(header.expressionsFor).map(header.column).sorted: _*))
  }

  def join(other: RelationalCypherRecords[T], joinType: JoinType, joinExprs: (Expr, Expr)*): RelationalCypherRecords[T] = {
    val joinCols = joinExprs.map { case (l, r) => header.column(l) -> header.column(r) }
    val joinHeader = header ++ other.header
    val joinData = table.join(other.table, joinType, joinCols: _*)
    from(joinHeader, joinData)
  }
}

/**
  * An entity table describes how to map an input data frame to a Cypher entity (i.e. nodes or relationships).
  */
trait EntityTable[T <: FlatRelationalTable[T]] extends RelationalCypherRecords[T] {

  verify()

  def schema: Schema

  def mapping: EntityMapping

  def header: RecordHeaderNew = mapping match {
    case n: NodeMapping => headerFrom(n)
    case r: RelationshipMapping => headerFrom(r)
  }

  protected def verify(): Unit = {
    mapping.idKeys.foreach(key => table.verifyColumnType(key, CTInteger, "id key"))
    if (table.columns != mapping.allSourceKeys) throw IllegalArgumentException(
      s"Columns: ${mapping.allSourceKeys.mkString(", ")}",
      s"Columns: ${table.columns.mkString(", ")}",
      s"Use CAPS[Node|Relationship]Table#fromMapping to create a valid EntityTable")
  }

  protected def headerFrom(nodeMapping: NodeMapping): RecordHeaderNew = {
    val nodeVar = Var("")(nodeMapping.cypherType)

    val exprToColumn = Map[Expr, String](nodeMapping.id(nodeVar)) ++
      nodeMapping.optionalLabels(nodeVar) ++
      nodeMapping.properties(nodeVar, table.columnType)

    RecordHeaderNew(exprToColumn)
  }

  protected def headerFrom(relationshipMapping: RelationshipMapping): RecordHeaderNew = {
    val relVar = Var("")(relationshipMapping.cypherType)

    val exprToColumn = Map[Expr, String](
      relationshipMapping.id(relVar),
      relationshipMapping.startNode(relVar),
      relationshipMapping.endNode(relVar)) ++ relationshipMapping.properties(relVar, table.columnType)

    RecordHeaderNew(exprToColumn)
  }
}

object RelationalEntityMapping {

  implicit class EntityMappingOps(val mapping: EntityMapping) {

    def id(v: Var): (Var, String) = v -> mapping.sourceIdKey

    def properties(
      v: Var,
      columnToCypherType: Map[String, CypherType]
    ): Map[Property, String] = mapping.propertyMapping.map {
      case (key, sourceColumn) => Property(v, PropertyKey(key))(columnToCypherType(sourceColumn)) -> sourceColumn
    }
  }

  implicit class NodeMappingOps(val mapping: NodeMapping) {

    def optionalLabels(node: Var): Map[HasLabel, String] = mapping.optionalLabelMapping.map {
      case (label, sourceColumn) => HasLabel(node, Label(label))(CTBoolean) -> sourceColumn
    }
  }

  implicit class RelationshipMappingOps(val mapping: RelationshipMapping) {

    def startNode(rel: Var): (StartNode, String) = StartNode(rel)(CTNode) -> mapping.sourceStartNodeKey

    def endNode(rel: Var): (EndNode, String) = EndNode(rel)(CTNode) -> mapping.sourceEndNodeKey
  }

}
