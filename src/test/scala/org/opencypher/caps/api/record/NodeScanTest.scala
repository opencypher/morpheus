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
package org.opencypher.caps.api.record

import org.opencypher.caps.CAPSTestSuite
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.types.{CTInteger, CTString}

class NodeScanTest extends CAPSTestSuite {

  test("test schema creation") {
    val nodeScan = NodeScan.on("p" -> "ID") {
      _.build
      .withImpliedLabel("A")
      .withImpliedLabel("B")
      .withOptionalLabel("C" -> "IS_C")
      .withPropertyKey("foo" -> "FOO")
      .withPropertyKey("bar" -> "BAR")
    }.from(CAPSRecords.create(
      Seq("ID", "IS_C", "FOO", "BAR"),
      Seq(
        (1, true, "Mats", 23)
      )
    ))

    nodeScan.schema should equal (Schema.empty
      .withImpliedLabel("A","B")
      .withImpliedLabel("B","A")
      .withImpliedLabel("C","A")
      .withImpliedLabel("C","B")
      .withLabelCombination("A","C")
      .withLabelCombination("B","C")
      .withNodePropertyKeys("A")("foo" -> CTString.nullable, "bar" -> CTInteger)
      .withNodePropertyKeys("B")("foo" -> CTString.nullable, "bar" -> CTInteger)
    )
  }
}
