package org.opencypher.spark.api.io.fs.hdfs

import org.apache.hadoop.fs.{FileSystem, Path}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.FileBasedDataSource

abstract class HdfsDataSource(
  override val rootPath: Path,
  fileSystem: FileSystem)(implicit val session: CAPSSession)
  extends FileBasedDataSource[Path] with HdfsAdapter
