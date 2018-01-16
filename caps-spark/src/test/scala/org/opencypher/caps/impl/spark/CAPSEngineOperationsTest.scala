/*
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
package org.opencypher.caps.impl.spark

import org.opencypher.caps.ir.api.expr.{Expr, Not, Var}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.caps.test.CAPSTestSuite

class CAPSEngineOperationsTest extends CAPSTestSuite {

  import operations._

  def base = CAPSGraph.empty(caps)

  test("filter operation on records") {

    val given = CAPSRecords.create(
      Seq("ID", "IS_SWEDE", "NAME"),
      Seq(
        (1L, true, "Mats"),
        (2L, false, "Martin"),
        (3L, false, "Max"),
        (4L, false, "Stefan")
      ))

    val result = base.filter(given, Var("IS_SWEDE")(CTBoolean))

    result.toDF().count() should equal(1L)
  }

  test("select operation on records") {
    val given = CAPSRecords.create(
      Seq("ID", "IS_SWEDE", "NAME"),
      Seq(
        (1L, true, "Mats"),
        (2L, false, "Martin"),
        (3L, false, "Max"),
        (4L, false, "Stefan")
      ))

    val result = base.select(given, IndexedSeq(Var("ID")(CTInteger), Var("NAME")(CTString)))

    result shouldMatch CAPSRecords.create(
      Seq("ID", "NAME"),
      Seq(
        (1L, "Mats"),
        (2L, "Martin"),
        (3L, "Max"),
        (4L, "Stefan")
      ))
  }

  test("project operation on records") {
    val given = CAPSRecords.create(
      Seq("ID", "IS_SWEDE", "NAME"),
      Seq(
        (1L, true, "Mats"),
        (2L, false, "Martin"),
        (3L, false, "Max"),
        (4L, false, "Stefan")
      ))

    val expr: Expr = Not(Var("IS_SWEDE")(CTBoolean))(CTBoolean)
    val result = base.project(given, expr)

    result shouldMatchOpaquely CAPSRecords.create(
      Seq("ID", "IS_SWEDE", "NAME", "NOT IS_SWEDE"),
      Seq(
        (1L, true, "Mats", false),
        (2L, false, "Martin", true),
        (3L, false, "Max", true),
        (4L, false, "Stefan", true)
      ))
  }

  test("project operation with alias on records") {
    val given = CAPSRecords.create(
      Seq("ID", "IS_SWEDE", "NAME"),
      Seq(
        (1L, true, "Mats"),
        (2L, false, "Martin"),
        (3L, false, "Max"),
        (4L, false, "Stefan")
      ))

    val exprVar = Not(Var("IS_SWEDE")(CTBoolean))(CTBoolean) -> Var("IS_NOT_SWEDE")(CTBoolean)
    val result = base.alias(given, exprVar)

    result shouldMatchOpaquely CAPSRecords.create(
      Seq("ID", "IS_SWEDE", "NAME", "IS_NOT_SWEDE"),
      Seq(
        (1L, true, "Mats", false),
        (2L, false, "Martin", true),
        (3L, false, "Max", true),
        (4L, false, "Stefan", true)
      ))
  }
}
