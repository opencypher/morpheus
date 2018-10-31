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
package org.opencypher.spark.examples

import java.io.File
import java.nio.file.Paths

import org.apache.hadoop.fs.PathNotFoundException
import org.opencypher.okapi.testing.BaseTestSuite

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

class MetaTest extends BaseTestSuite {

  val readmeName = "README.md"
  val moduleName = "spark-cypher-examples"

  private val rootFolderPath = findRootFolderPath(Paths.get(".").toAbsolutePath.normalize.toString)
  private val examplePath = CaseClassExample.getClass.getName.dropRight(1).replace(".", File.separator)
  private val exampleClassName = examplePath.substring(examplePath.lastIndexOf(File.separator) + 1) + ".scala"
  private val examplePackagePath = examplePath.substring(0, examplePath.lastIndexOf(File.separator))
  private val readmePath = Paths.get(rootFolderPath, readmeName).toString

  private def absolutePackagePath(scope: String) =
    Paths.get(rootFolderPath, moduleName, Paths.get("src", scope, "scala").toString, examplePackagePath).toString

  private val caseClassExamplePath =
    Paths.get(absolutePackagePath("main"), exampleClassName).toString

  it("should exist a test for each CAPS example") {
    val exampleClassPrefixes = new File(absolutePackagePath("main")).listFiles()
      .map(_.getName)
      .filter(_.contains("Example"))
      .map(example => example.substring(0, example.indexOf("Example")))

    val exampleTestClassPrefixes = new File(absolutePackagePath("test")).listFiles()
      .map(_.getName)
      .filter(_.contains("Example"))

    exampleClassPrefixes.forall(prefix => exampleTestClassPrefixes.exists(_.startsWith(prefix))) shouldBe true
  }

  /**
    * Tests whether the README example is aligned with the code contained in [[CaseClassExample]].
    */
  it("the code in the readme matches the example") {
    val readmeLines = Source.fromFile(readmePath).getLines.toVector
    val readmeSourceCodeBlocks = extractMarkdownScalaSourceBlocks(readmeLines).map(_.canonical).toSet

    val exampleSourceCodeLines = Source.fromFile(caseClassExamplePath).getLines.toVector
    val exampleSourceCode = ScalaSourceCode(exampleSourceCodeLines).canonical

    readmeSourceCodeBlocks should contain(exampleSourceCode)
  }

  case class ScalaSourceCode(lines: Vector[String]) {
    def canonical: Vector[String] = lines
      .dropWhile(line => !line.startsWith("import")) // Drop license and everything else before the first import
      .filterNot(_.contains("// tag::")).filterNot(_.contains("// end::")) // Filter documentation tags
      .filterNot(_ == "") // Filter empty lines

    override def toString: String = lines.mkString("\n")
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
    * Check by testing if the CONTRIBUTING.adoc file can be found. This works even if the root folder has a different name.
    */
  def isRootFolderPath(path: String): Boolean = Paths.get(path, "CONTRIBUTING.adoc").toFile.exists

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
