package org.opencypher.spark.api.io.fs

import org.opencypher.spark.api.CAPSSession

trait FileSystemOperations[Path] {

  implicit val session: CAPSSession

  protected def listDirectories(path: Path): List[String]

  protected def deleteDirectory(path: Path): Unit

  protected def readFile(path: Path): String

  protected def writeFile(path: Path, content: String): Unit

}
