package org.opencypher.spark.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData}

import ImplicitHelpers._

object NorthwindDB {

  val schema: String = "NORTHWIND"

  // list of tables
  val Employees = s"$schema.Employees"

  val jdbcUrl: String = s"jdbc:h2:mem:$schema.db;INIT=CREATE SCHEMA IF NOT EXISTS $schema;DB_CLOSE_DELAY=30;"

  def init(): Unit = {

    // load driver
    Class.forName("org.h2.Driver")

    val connection = DriverManager.getConnection(jdbcUrl)
    connection.setSchema(schema)

    // create the SQL db schema
    connection.run(NorthwindHelpers.schemaSql)

    // validate all tables present

    // populate tables with data
    connection.run(NorthwindHelpers.dataSql)

    // create views that hide problematic columns
    connection.run(NorthwindHelpers.viewSql)

    // print all table metadata (for debugging)
    //  println(connection.getMetaData.getTables(null, null, null, Array("TABLE")).toMaps)
  }

}

object ImplicitHelpers {
  implicit class RichConnection(c: Connection) {

    def run(s: String): Boolean = {
      val statement = c.createStatement()
      statement.execute(s)
    }

  }

  implicit class RichResultSet(rs: ResultSet) {

    lazy val metadata: ResultSetMetaData = rs.getMetaData

    lazy val columnNames: Vector[String] = {
      val columnNames = for {
        i <- 1 to rs.getMetaData.getColumnCount
      } yield metadata.getColumnName(i)
      columnNames.toVector
    }

    def rows: Iterator[ResultSet] = {
      new Iterator[ResultSet] {
        def hasNext: Boolean = rs.next()

        def next(): ResultSet = rs
      }
    }

    def toMap: Map[String, AnyRef] = {
      columnNames
        .filter(rs.findColumn(_) >= 0)
        .map { cn =>
          cn -> rs.getObject(cn)
        }.toMap
    }

    def toMaps: Vector[Map[String, AnyRef]] = {
      rows.map(_.toMap).toVector
    }

  }
}
