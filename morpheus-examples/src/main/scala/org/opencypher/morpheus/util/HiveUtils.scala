package org.opencypher.morpheus.util

import java.io.File

import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

object HiveUtils {
  def hiveExampleSettings: Seq[(String, String)] = {
    val sparkWarehouseDir = new File(s"spark-warehouse_${System.currentTimeMillis}").getAbsolutePath
    Seq(
      // ------------------------------------------------------------------------
      // Create a new unique local spark warehouse dir for every run - idempotent
      // ------------------------------------------------------------------------
      ("spark.sql.warehouse.dir", sparkWarehouseDir),
      // -----------------------------------------------------------------------------------------------------------
      // Create an in-memory Hive Metastore (only Derby supported for this mode)
      // This is to avoid creating a local HIVE "metastore_db" on disk which needs to be cleaned up before each run,
      // e.g. avoids database and table already exists exceptions on re-runs - not to be used for production.
      // -----------------------------------------------------------------------------------------------------------
      ("javax.jdo.option.ConnectionURL", s"jdbc:derby:memory:;databaseName=metastore_db;create=true"),
      ("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver"),
      // ------------------------------------------------------------------------------------------------------------
      // An alternative way of enabling Spark Hive Support (e.g. you could use enableHiveSupport on the SparkSession)
      // ------------------------------------------------------------------------------------------------------------
      ("hive.metastore.warehouse.dir", s"warehouse_${System.currentTimeMillis}"),
      (CATALOG_IMPLEMENTATION.key, "hive") // Enable hive
    )
  }
}
