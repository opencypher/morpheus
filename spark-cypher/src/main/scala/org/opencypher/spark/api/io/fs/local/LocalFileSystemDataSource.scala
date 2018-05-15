package org.opencypher.spark.api.io.fs.local

import java.nio.file._

import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.FileBasedDataSource

abstract class LocalFileSystemDataSource(
  override val rootPath: Path,
  fileSystem: FileSystem = FileSystems.getDefault)(implicit val session: CAPSSession)
  extends FileBasedDataSource[Path] with LocalFileSystemAdapter
