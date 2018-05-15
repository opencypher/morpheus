package org.opencypher.spark.api.io.fs

import java.io._
import java.nio.file.{Files, Path => NioPath}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path => HDFSPath}
import org.opencypher.spark.api.CAPSSession

import scala.collection.JavaConverters._

trait FileSystemAdapter[Path] {

  implicit val session: CAPSSession

  protected def listDirectories(path: Path): List[String]

  protected def deleteDirectory(path: Path): Unit

  protected def readFile(path: Path): String

  protected def writeFile(path: Path, content: String): Unit

}




