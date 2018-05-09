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

import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.okapi.ir.api.expr.{Expr, Not, Var}
import org.opencypher.spark.testing.CAPSTestSuite

class CAPSEngineOperationsTest extends CAPSTestSuite {

  import operations._

  implicit val extendedCaps = caps.asInstanceOf[CAPSSessionImpl]

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

    result.size should equal(1L)
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

    val result = base.select(given, List(Var("ID")(CTInteger), Var("NAME")(CTString)))

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
