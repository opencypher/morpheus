package org.opencypher.spark.api.io.sql
import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.sql.DataFrame

object SqlTestUtils {

  implicit class ConnOps(conn: Connection) {
    def run[T](code: Statement => T): T = {
      val stmt = conn.createStatement()
      try { code(stmt) } finally { stmt.close() }
    }
    def execute(sql: String): Boolean = conn.run(_.execute(sql))
    def query(sql: String): ResultSet = conn.run(_.executeQuery(sql))
    def update(sql: String): Int = conn.run(_.executeUpdate(sql))
  }

  def withConnection[T](cfg: SqlDataSourceConfig)(code: Connection => T): T = {
    Class.forName(cfg.jdbcDriver.get)
    val conn = DriverManager.getConnection(cfg.jdbcUri.get)
    try { code(conn) } finally { conn.close() }
  }

  implicit class DataframeSqlOps(df: DataFrame) {
    def saveAsSqlTable(cfg: SqlDataSourceConfig, tableName: String): Unit =
      df.write
        .format("jdbc")
        .option("url", cfg.jdbcUri.getOrElse(throw SqlDataSourceConfigException("Missing JDBC URI")))
        .option("driver", cfg.jdbcDriver.getOrElse(throw SqlDataSourceConfigException("Missing JDBC Driver")))
        .option("fetchSize", cfg.jdbcFetchSize)
        .option("dbtable", tableName)
        .save()
  }

}
