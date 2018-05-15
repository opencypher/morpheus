package org.opencypher.spark.api.io.fs

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.opencypher.spark.api.io.util.FileSystemUtils.using

trait CAPSFileSystem {

  def listDirectories(path: String): List[String]

  def deleteDirectory(path: String): Unit

  def readFile(path: String): String

  def writeFile(path: String, content: String): Unit

}

object DefaultFileSystem {

  implicit class HadoopFileSystemAdapter(fileSystem: FileSystem) extends CAPSFileSystem {

    def listDirectories(path: String): List[String] = {
      fileSystem.listStatus(new Path(path))
        .filter(_.isDirectory)
        .map(_.getPath.getName)
        .toList
    }

    def deleteDirectory(path: String): Unit = {
      fileSystem.delete(new Path(path), /* recursive = */ true)
    }

    def readFile(path: String): String = {
      using(new BufferedReader(new InputStreamReader(fileSystem.open(new Path(path)), "UTF-8"))) { reader =>
        def readLines = Stream.cons(reader.readLine(), Stream.continually(reader.readLine))
        readLines.takeWhile(_ != null).mkString
      }
    }

    def writeFile(path: String, content: String): Unit = {
      using(fileSystem.create(new Path(path))) { outputStream =>
        using(new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))) { bufferedWriter =>
          bufferedWriter.write(content)
        }
      }
    }
  }

}
