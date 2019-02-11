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
package org.opencypher.spark.testing.fixture

import org.apache.spark.sql.Row
import org.opencypher.okapi.ir.api.expr.Expr
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable
import org.opencypher.spark.testing.CAPSTestSuite

trait RecordsVerificationFixture {

  self: CAPSTestSuite  =>

  protected def verify(records: RelationalCypherRecords[DataFrameTable], expectedExprs: Seq[Expr], expectedData: Bag[Row]): Unit = {
    val df = records.table.df
    val header = records.header
    val expectedColumns = expectedExprs.map(header.column)
    df.columns.length should equal(expectedColumns.size)
    df.columns.toSet should equal(expectedColumns.toSet)

    // Array equality is based on reference, not structure. Hence, we need to convert to lists.
    val actual = df.select(expectedColumns.head, expectedColumns.tail: _*).collect().map { r =>
      Row(r.toSeq.map {
        case c: Array[_] => c.toList
        case other => other
      }: _*)
    }.toBag

    actual should equal(expectedData)
  }
}
