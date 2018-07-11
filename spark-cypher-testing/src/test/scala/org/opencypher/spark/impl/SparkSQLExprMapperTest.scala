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
package org.opencypher.spark.impl

import java.util.Collections

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.expr.{Expr, Subtract, Var}
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.testing.fixture.SparkSessionFixture

import scala.language.implicitConversions

class SparkSQLExprMapperTest extends BaseTestSuite with SparkSessionFixture {

  val vA = Var("a")()
  val vB = Var("b")()
  val header: RecordHeader = RecordHeader.from(vA, vB)

  test("can map subtract") {
    val expr = Subtract(Var("a")(), Var("b")())()
    convert(expr, header.withExpr(expr).withAlias(expr as Var("foo")())) should equal(
    df.col(header.column(vA)) - df.col(header.column(vB))
    )
  }

  private def convert(expr: Expr, header: RecordHeader = header): Column = {
    expr.asSparkSQLExpr(header, df, CypherMap.empty)
  }
  val df: DataFrame = sparkSession.createDataFrame(
    Collections.emptyList[Row](),
    StructType(Seq(StructField(header.column(vA), IntegerType), StructField(header.column(vB), IntegerType))))

  implicit def extractRecordHeaderFromResult[T](tuple: (RecordHeader, T)): RecordHeader = tuple._1
}
