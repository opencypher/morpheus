/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.spark.examples

import java.io.File
import java.nio.file.Paths

import org.apache.hadoop.fs.PathNotFoundException
import org.scalatest.{FunSuite, Matchers}

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

/**
  * Tests whether the README example is aligned with the code contained in [[CaseClassExample]].
  */
class ReadmeTest extends FunSuite with Matchers {

  val sep = File.separator

  val readmeName = "README.md"
  val rootFolderPath = findRootFolderPath(Paths.get(".").toAbsolutePath.normalize.toString)
  val readmePath = s"${rootFolderPath}${sep}${readmeName}"

  val testSourceFolderPath = s"${sep}src${sep}main${sep}scala${sep}"
  val moduleName = "caps-examples"
  val examplePackagePath = CaseClassExample.getClass.getName.dropRight(1).replace(".", sep)
  val examplePath = s"$rootFolderPath$sep$moduleName$testSourceFolderPath$examplePackagePath.scala"

  test("running the example code") {
    CaseClassExample.main(Array.empty[String])
  }

  test("the code in the readme matches the example") {
    val readmeLines = Source.fromFile(readmePath).getLines.toVector
    val readmeSourceCodeBlocks = extractMarkdownScalaSourceBlocks(readmeLines).map(_.canonical).toSet

    val exampleSourceCodeLines = Source.fromFile(examplePath).getLines.toVector
    val exampleSourceCode = ScalaSourceCode(exampleSourceCodeLines).canonical

    readmeSourceCodeBlocks should contain(exampleSourceCode)
  }

  case class ScalaSourceCode(lines: Vector[String]) {
    def canonical: Vector[String] = lines
      .dropWhile(line => !line.startsWith("import")) // Drop license and everything else before the first import
      .filterNot(_ == "") // Filter empty lines

    override def toString = lines.mkString("\n")
  }

  /**
    * Find the root folder path even if the tests are executed in a child path.
    */
  def findRootFolderPath(potentialChildFolderPath: String): String = {
    @tailrec def recFindRootFolderPath(folder: String): String = {
      if (isRootFolderPath(folder)) {
        folder
      } else {
        recFindRootFolderPath(new File(folder).getParent)
      }
    }

    Try(recFindRootFolderPath(potentialChildFolderPath)).getOrElse(
      throw new PathNotFoundException(
        s"Directory $potentialChildFolderPath is not a sub-folder of the project root directory."))
  }

  /**
    * Check by testing if the README.md file can be found. This works even if the root folder has a different name.
    */
  def isRootFolderPath(path: String): Boolean = new File(s"$path${sep}$readmeName").exists

  def extractMarkdownScalaSourceBlocks(lines: Vector[String]): Seq[ScalaSourceCode] = {
    val currentParsingState: (Vector[ScalaSourceCode], Option[Vector[String]]) = (Vector.empty, None)
    val sourceCodeSnippets = lines.foldLeft(currentParsingState) {
      case ((sourceBlocks, currentBlock), currentLine) =>
        currentBlock match {
          case Some(block) =>
            if (currentLine == "```") {
              (sourceBlocks :+ ScalaSourceCode(block), None)
            } else {
              (sourceBlocks, Some(block :+ currentLine))
            }
          case None =>
            if (currentLine == "```scala") {
              (sourceBlocks, Some(Vector.empty))
            } else {
              (sourceBlocks, None)
            }
        }
    }._1
    sourceCodeSnippets
  }
}
