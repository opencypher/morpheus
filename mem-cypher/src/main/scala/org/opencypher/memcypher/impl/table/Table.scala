package org.opencypher.memcypher.impl.table

import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.relational.api.table.{Table => RelationalTable}
import org.opencypher.okapi.relational.impl.planning.{JoinType, Order}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object Table {
  def empty: Table = Table(schema = Schema.empty, data = Seq.empty)
}

case class Table(schema: Schema, data: Seq[Row]) extends RelationalTable[Table] {

  private implicit val implicitSchema: Schema = schema

  override def select(cols: String*): Table = {
    val columnsWithIndex = cols.map(col => col -> schema.fieldIndex(col))

    val newSchema = schema.select(cols)
    val newLength = cols.length

    val newData = data.map { row =>
      val newValues = Array.ofDim[Any](newLength)
      for (i <- 0 until newLength) {
        newValues(i) = row.get(columnsWithIndex(i)._2)
      }
      Row(newValues)
    }
    Table(newSchema, newData)
  }

  override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): Table =
    copy(data = data.filter(_.evaluate(expr).asInstanceOf[Boolean]))

  override def drop(cols: String*): Table = ???

  override def join(
    other: Table,
    joinType: JoinType,
    joinCols: (String, String)*
  ): Table = ???

  override def unionAll(other: Table): Table = ???

  override def orderBy(sortItems: (Expr, Order)*)
    (implicit header: RecordHeader, parameters: CypherMap): Table = ???

  override def skip(n: Long): Table = ???

  override def limit(n: Long): Table = ???

  override def distinct: Table = ???

  override def group(by: Set[Var], aggregations: Set[(Aggregator, (String, CypherType))])
    (implicit header: RecordHeader, parameters: CypherMap): Table = ???

  override def withColumns(columns: (Expr, String)*)
    (implicit header: RecordHeader, parameters: CypherMap): Table = {

    val newSchema = columns.foldLeft(schema) {
      case (currentSchema, (expr, columnName)) => currentSchema.withColumn(columnName, expr.cypherType)
    }

    val newData = data.map(row => Row(row.values ++ columns.map(_._1).map(row.evaluate)))

    Table(newSchema, newData)
  }

  override def withColumnRenamed(oldColumn: String, newColumn: String): Table =
    Table(schema.withColumnRenamed(oldColumn, newColumn), data)

  override def show(rows: Int): Unit = ???

  override def physicalColumns: Seq[String] =
    schema.columns.map(_.name)

  override def columnsFor(returnItem: String): Set[String] = ???

  override def columnType: Map[String, CypherType] =
    schema.columns.map(column => column.name -> column.dataType).toMap

  override def rows: Iterator[String => CypherValue] = ???

  override def size: Long = ???
}






