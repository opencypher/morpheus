package org.opencypher.spark.api.io.csv

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.hdfs.HdfsDataSource
import org.opencypher.spark.api.io.json.JsonHdfsDefaultPaths

object HdfsCsvDataSource {

  def apply(rootPath: String)(implicit session: CAPSSession): HdfsCsvDataSource = {
    apply(new Path(rootPath))
  }

  def apply(rootPath: Path)(implicit session: CAPSSession): HdfsCsvDataSource = {
    HdfsCsvDataSource(rootPath, FileSystem.get(session.sparkSession.sparkContext.hadoopConfiguration))
  }

}

case class HdfsCsvDataSource(
  override val rootPath: Path,
  fileSystem: FileSystem)(implicit session: CAPSSession)
  extends HdfsDataSource(rootPath, fileSystem) with JsonHdfsDefaultPaths {

  override protected def readTable(path: Path, schema: StructType): DataFrame = {
    session.sparkSession.read.schema(schema).csv(path.toString)
  }

  override protected def writeTable(path: Path, table: DataFrame): Unit = {
    table.write.csv(path.toString)
  }

}
