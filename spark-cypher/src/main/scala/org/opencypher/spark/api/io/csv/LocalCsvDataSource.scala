package org.opencypher.spark.api.io.csv

import java.nio.file.Path

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.local.LocalFileSystemDataSource
import org.opencypher.spark.api.io.json.JsonNioDefaultPaths

case class LocalCsvDataSource(override val rootPath: Path)(implicit session: CAPSSession)
  extends LocalFileSystemDataSource(rootPath) with JsonNioDefaultPaths {

  override protected def readTable(path: Path, schema: StructType): DataFrame = {
    session.sparkSession.read.schema(schema).csv(path.toString)
  }

  override protected def writeTable(path: Path, table: DataFrame): Unit = {
    table.write.csv(path.toString)
  }

}
