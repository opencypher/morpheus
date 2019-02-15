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
package org.opencypher.spark.examples

import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.api.value.CypherValue.Format._
import org.opencypher.okapi.impl.util.TablePrinter.toTable
import org.opencypher.spark.api.io.sql.SqlDataSourceConfig
import org.opencypher.spark.testing.utils.H2Utils
import org.opencypher.spark.util.NorthwindDB

import scala.io.Source

class NorthwindJdbcExampleTest extends ExampleTest {
  it("runs JdbcSqlGraphSourceExample") {
    validate(
      NorthwindJdbcExample.main(Array.empty),
      getClass.getResource("/example_outputs/NorthwindJdbcExample.out").toURI
    )
  }

  it("verifies the correctness of the output") {
    val dataSourceConfig = SqlDataSourceConfig.Jdbc(
      url = "jdbc:h2:mem:NORTHWIND.db;DB_CLOSE_DELAY=30;",
      driver = "org.h2.Driver"
    )

    NorthwindDB.init(dataSourceConfig)

    val sqlResult = H2Utils.withConnection(dataSourceConfig) { conn =>
      val stmt = conn.createStatement()
      var resultTuples = List.empty[Seq[CypherValue]]
      val result = stmt.executeQuery(
        """
          |SELECT
          | ord.CustomerID AS customer,
          | ord.OrderDate AS orderedAt,
          | emp.LastName AS handledBy,
          | emp.Title AS employee
          |FROM
          | NORTHWIND.Employees emp,
          | NORTHWIND.Employees man,
          | NORTHWIND.Orders ord
          |WHERE
          | emp.ReportsTo = man.EmployeeID
          | AND man.EmployeeID = ord.EmployeeID
          |ORDER BY
          | ord.OrderDate,
          | emp.LastName,
          | ord.CustomerID
          |LIMIT 50
          |""".stripMargin)

      while (result.next()) {
        val tuple = Seq(
          CypherValue(result.getString(1)),
          CypherValue(result.getString(2)),
          CypherValue(result.getString(3)),
          CypherValue(result.getString(4))
        )
        resultTuples = resultTuples :+ tuple
      }
      toTable(Seq("customer", "orderedAt", "handledBy", "employee"), resultTuples.map(row => row.map(_.toCypherString()))).dropRight(1)
    }

    // We only need the last query result from the expected example output
    val cypherResult = Source
      .fromFile(getClass.getResource("/example_outputs/NorthwindJdbcExample.out").toURI)
      .getLines()
      .toList
      .takeRight(55)
      .mkString("\n")

    sqlResult should equal(cypherResult)
  }
}
