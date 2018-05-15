package org.opencypher.spark.api.io.fs.local

import java.io._
import java.nio.file.{Files, Path => NioPath}

import org.apache.commons.io.FileUtils
import org.opencypher.spark.api.io.fs.FileSystemAdapter

import scala.collection.JavaConverters._

trait LocalFileSystemAdapter extends FileSystemAdapter[NioPath] {

  override protected def listDirectories(path: NioPath): List[String] = {
    Files.list(path).iterator.asScala
      .filter(Files.isDirectory(_))
      .map(_.getFileName.toString)
      .toList
  }

  override protected def deleteDirectory(path: NioPath): Unit = {
    FileUtils.deleteDirectory(path.toFile)
  }

  override protected def readFile(path: NioPath): String = {
    new String(Files.readAllBytes(path))
  }

  override protected def writeFile(path: NioPath, content: String): Unit = {
    val file = new File(path.toString)
    file.getParentFile.mkdirs
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(content)
    bw.close()
  }

}
