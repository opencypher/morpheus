package org.opencypher.spark.api.io.fs.hdfs

import java.io._

import org.apache.hadoop.fs.{FileSystem, Path => HDFSPath}
import org.opencypher.spark.api.io.fs.FileSystemAdapter

trait HdfsAdapter extends FileSystemAdapter[HDFSPath] {

  val fileSystem: FileSystem

  override protected def listDirectories(path: HDFSPath): List[String] = {
    fileSystem.listStatus(path)
      .filter(_.isDirectory)
      .map(_.getPath.getName)
      .toList
  }

  override protected def deleteDirectory(path: HDFSPath): Unit = {
    fileSystem.delete(path, /* recursive = */ true)
  }

  override protected def readFile(path: HDFSPath): String = {
    val stream = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    def readLines = Stream.cons(stream.readLine(), Stream.continually(stream.readLine))
    readLines.takeWhile(_ != null).mkString
  }

  override protected def writeFile(path: HDFSPath, content: String): Unit = {
    val outputStream = fileSystem.create(path)
    val bw = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))
    bw.write(content)
    bw.close()
  }

}
