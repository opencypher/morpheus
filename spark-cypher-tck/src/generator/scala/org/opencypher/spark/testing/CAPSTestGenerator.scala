/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.testing

import java.io.File

import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.tck.test.{AcceptanceTestGenerator, SpecificNamings}

object CAPSTestGenerator extends App {
  val imports = List("import org.opencypher.spark.testing.CAPSTestSuite",
    "import org.opencypher.spark.testing.support.creation.caps.CAPSScanGraphFactory")
  val renamings = SpecificNamings("CAPSScanGraphFactory", "initGraph", "apply(InMemoryTestGraph.empty)",
    "CAPSTestSuite", "org.opencypher.spark.testing")
  val generator = AcceptanceTestGenerator(imports, renamings, addGitIgnore = true, checkSideEffects = false)

  if (args.isEmpty) {
    val defaultOutDir = new File("spark-cypher-tck/src/test/scala/org/opencypher/spark/testing/")
    val defaultResFiles = new File("spark-cypher-tck/src/test/resources/").listFiles()
    generator.generateAllScenarios(defaultOutDir, defaultResFiles)
  }
  else {
    //parameter names specified in gradle task
    val (outDir, resFiles) = (new File(args(0)), Option(new File(args(1)).listFiles()))

    resFiles match {
      case Some(files) =>
        val scenarioNames = args(2)
        if (scenarioNames.nonEmpty)
          generator.generateGivenScenarios(outDir, files, scenarioNames.split('|'))
        else
          generator.generateAllScenarios(outDir, files)
      case None => throw IllegalArgumentException("resource Dir does not exist")
    }
  }
}
