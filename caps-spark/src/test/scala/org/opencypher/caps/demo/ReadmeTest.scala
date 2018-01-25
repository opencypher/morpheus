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
package org.opencypher.caps.demo

import java.io.File

import org.opencypher.caps.test.BaseTestSuite

import scala.io.Source

class ReadmeTest extends BaseTestSuite {

  test("running the example code") {
    Example.main(Array.empty[String])
  }

  test("the code in the readme matches the example") {
    val readmeLines = Source.fromFile("README.md").getLines.toVector
    val sourceCodeBlocks = extractMarkdownScalaSourceBlocks(readmeLines).toSet

    val sep = File.separatorChar
    val modulePrefix = s".${sep}caps-spark${sep}src${sep}test${sep}scala${sep}"
    val packagePath = Example.getClass.getName.dropRight(1).replace('.', sep)
    val pathToSource = modulePrefix + packagePath + ".scala"
    val exampleSourceLines = Source.fromFile(pathToSource).
      getLines.dropWhile(line => !line.startsWith("import")).filterNot(_ == "").toVector
    val exampleSourceText = ScalaSourceCode(exampleSourceLines)

    sourceCodeBlocks.map(_.canonical) should contain(exampleSourceText.canonical)
  }

  case class ScalaSourceCode(lines: Vector[String]) {
    def canonical: Vector[String] = {
      lines.dropWhile(line => !line.startsWith("import")).filterNot(_ == "")
    }

    override def toString = lines.mkString("\n")
  }

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
