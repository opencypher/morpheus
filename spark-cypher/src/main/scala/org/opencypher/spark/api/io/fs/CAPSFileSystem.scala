/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
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
