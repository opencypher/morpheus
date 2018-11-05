package org.opencypher.spark.util

import scala.io.Source

object NorthwindHelpers {

  private def readResourceAsString(name: String, filterNot: String => Boolean = _ => false): String =
    Source.fromFile(getClass.getResource(name).toURI)
      .getLines()
      .filterNot(_.startsWith("#"))
      .filterNot(filterNot)
      .mkString("\n")

  def schemaSql: String = readResourceAsString(sqlSchemaFile, _.startsWith("CREATE INDEX"))
  def dataSql: String = readResourceAsString(sqlDataFile)
  def viewSql: String = readResourceAsString(sqlViewFile)

  private val dir = "/northwind/sql"

  val sqlSchemaFile = s"$dir/northwind_schema.sql"
  val sqlViewFile = s"$dir/northwind_views.sql"
  val sqlDataFile = s"$dir/northwind_data.sql"
  val sqlDdlFile = s"$dir/northwind.ddl.sql"

}
