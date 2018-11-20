package org.opencypher.memcypher.impl.table

import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.impl.exception.IllegalArgumentException

object Schema {
  def empty: Schema = Schema(Array.empty)
}

case class Schema(columns: Array[ColumnSchema]) {

  def fieldIndex(name: String): Int = columns.map(_.name).indexOf(name)

  def withColumn(columnMeta: ColumnSchema): Schema = withColumn(columnMeta.name, columnMeta.dataType)

  def withColumn(name: String, dataType: CypherType): Schema = copy(columns = columns :+ ColumnSchema(name, dataType))

  def withColumnRenamed(oldName: String, newName: String): Schema = {
    val newColumns = columns.map {
      case columnMeta if columnMeta.name == oldName => ColumnSchema(newName, columnMeta.dataType)
      case columnMeta => columnMeta
    }
    copy(columns = newColumns)
  }

  def columnNames: Array[String] = columns.map(_.name)

  def select(names: Seq[String]): Schema = names.foldLeft(Schema.empty) {
    case (currentSchema, columnName) =>
      val columnMeta = columns
        .collectFirst { case colMeta if colMeta.name == columnName => colMeta }
        .getOrElse(columnNotFound(columnName))
      currentSchema.withColumn(columnMeta)
  }

  def ++(other: Schema): Schema = copy(columns = columns ++ other.columns)

  private def columnNotFound(name: String): Nothing =
    throw IllegalArgumentException(expected = s"existing column (one of: ${columnNames.mkString("[", ", ", "]")})", name)
}

case class ColumnSchema(name: String, dataType: CypherType)