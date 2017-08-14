/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.spark_legacy.impl

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{LongType, StringType}
import org.opencypher.spark_legacy.api.frame.EmbeddedRepresentation
import org.opencypher.spark.api.types.{CTInteger, CTString}
import org.opencypher.spark.{BaseTestSuite, SparkTestSession}

class StdCypherResultContainerTest extends BaseTestSuite with SparkTestSession {

  test("should output a nice table") {
    val signature = new StdFrameSignature(Map(
      StdField('name -> CTString) -> StdSlot('a, CTString, 0, EmbeddedRepresentation(StringType)),
      StdField('age -> CTInteger) -> StdSlot('b, CTInteger, 1, EmbeddedRepresentation(LongType))
    ))
    val frame = new StdCypherFrame[Product](signature) {
      override protected def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
        val tuples: Seq[Product] = Seq("Mats" -> 29l, "Stefan" -> 36l, "Third guy" -> 55l, "Nobody" -> -1l)
        context.session.createDataset(tuples)(context.productEncoder(signature.slots))
      }
    }

    val resultContainer = new StdCypherResultContainer(null, frame)(new StdRuntimeContext(session, Map.empty))

    val out = new StringWriter()
    resultContainer.print(new PrintWriter(out))
    out.toString should equal(
      """|+---------------------------------------------+
         || name                 | age                  |
         |+---------------------------------------------+
         || 'Mats'               | 29                   |
         || 'Stefan'             | 36                   |
         || 'Third guy'          | 55                   |
         || 'Nobody'             | -1                   |
         |+---------------------------------------------+
         |""".stripMargin)
  }
}
