import ammonite.ops._
import mill._
import mill.eval.Result
import mill.scalalib._

trait OkapiModule extends SbtModule {

  def scalaVersion = "2.11.12"

  def checkLicenses = T {
    val incorrectLicenses = allSourceFiles().filterNot { f: PathRef =>
      LicenseChecker.checkLicense(f.path.toString)
    }.toList
    incorrectLicenses match {
      case Nil =>
        T.ctx().log.info("All licenses are correct.")
        Result.Success(())
      case files =>
        Result.Failure(s"""Invalid license on files:\n${files.map(f => s"\t- ${f.path}").mkString("\n")}\n""")
    }
  }

  def moduleName: String

  def okapiTestDependencies = Seq(
    ivy"org.scalatest::scalatest:3.0.5"
  )

  def millSourcePath = super.millSourcePath / up / moduleName

  def ivyDeps = Agg(
    ivy"org.scala-lang:scala-reflect:${scalaVersion()}"
  )

  trait OkapiTests extends Tests {
    def ivyDeps = Agg(okapiTestDependencies: _*)

    def testFrameworks = Seq("org.scalatest.tools.Framework")
  }

}

object trees extends OkapiModule {

  def moduleName = "okapi-trees"

  def ivyDeps = Agg(
    ivy"org.scala-lang:scala-reflect:${scalaVersion()}"
  )

  object test extends OkapiTests

}

object api extends OkapiModule {

  def moduleName = "okapi-api"

  def moduleDeps = Seq(trees)

  def ivyDeps = Agg(
    ivy"org.typelevel::cats-core:1.0.1"
  )

  object test extends OkapiTests {
    def ivyDeps = Agg(okapiTestDependencies :+ ivy"org.mockito:mockito-all:1.10.19": _*)
  }

}

object LicenseChecker {

  val expectedLicense =
    """|/*
       | * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
       | *
       | * Licensed under the Apache License, Version 2.0 (the "License");
       | * you may not use this file except in compliance with the License.
       | * You may obtain a copy of the License at
       | *
       | *     http://www.apache.org/licenses/LICENSE-2.0
       | *
       | * Unless required by applicable law or agreed to in writing, software
       | * distributed under the License is distributed on an "AS IS" BASIS,
       | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       | * See the License for the specific language governing permissions and
       | * limitations under the License.
       | *
       | * Attribution Notice under the terms of the Apache License 2.0
       | *
       | * This work was created by the collective efforts of the openCypher community.
       | * Without limiting the terms of Section 6, any Derivative Work that is not
       | * approved by the public consensus process of the openCypher Implementers Group
       | * should not be described as “Cypher” (and Cypher® is a registered trademark of
       | * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
       | * proposals for change that have been documented or implemented should only be
       | * described as "implementation extensions to Cypher" or as "proposed changes to
       | * Cypher that are not yet approved by the openCypher community".
       | */""".stripMargin

  val licenseLines = expectedLicense.count {
    case '\n' => true
    case _ => false
  } + 1

  def checkLicense(path: String): Boolean = {
    val actualHeader = scala.io.Source.fromFile(path).getLines.take(licenseLines).mkString("\n")
    expectedLicense == actualHeader
  }

}
