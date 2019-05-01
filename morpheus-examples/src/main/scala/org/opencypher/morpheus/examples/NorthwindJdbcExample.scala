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
// tag::full-example[]
package org.opencypher.morpheus.examples

import org.opencypher.morpheus.api.io.sql.SqlDataSourceConfig
import org.opencypher.morpheus.api.{GraphSources, MorpheusSession}
import org.opencypher.morpheus.util.{App, NorthwindDB}
import org.opencypher.okapi.api.graph.Namespace

object NorthwindJdbcExample extends App {

  implicit val resourceFolder: String = "/northwind"

  // Initialise local Morpheus session
  implicit val morpheus: MorpheusSession = MorpheusSession.local()

  // define the root configuration directory for the SQL graph source
  // this holds the data source mappings files and the SQL DDL file
  // the latter contains the graph definitions and mappings from SQL tables that fill the graph with data

  val dataSourceConfig = SqlDataSourceConfig.Jdbc(
    url = "jdbc:h2:mem:NORTHWIND.db;DB_CLOSE_DELAY=30;",
    driver = "org.h2.Driver"
  )
  val sqlGraphSource = GraphSources
    .sql(resource("ddl/northwind.ddl").getFile)
    .withSqlDataSourceConfigs("H2" -> dataSourceConfig)

  // start up the SQL database
  NorthwindDB.init(dataSourceConfig)

  // register the SQL graph source with the session
  morpheus.registerSource(Namespace("sql"), sqlGraphSource)

  // print the number of nodes in the graph
  morpheus.cypher(
    """
      |FROM GRAPH sql.Northwind
      |MATCH (n)
      |RETURN count(n)
    """.stripMargin).records.show

  // print the schema of the graph
  println(morpheus.catalog.graph("sql.Northwind").schema.pretty)

  // run a simple query
  morpheus.cypher(
    """
      |FROM GRAPH sql.Northwind
      |MATCH (e:Employee)-[:REPORTS_TO]->(:Employee)<-[:HAS_EMPLOYEE]-(o:Order)
      |RETURN o.customerID AS customer, o.orderDate AS orderedAt, e.lastName AS handledBy, e.title AS employee
      |  ORDER BY orderedAt, handledBy, customer
      |  LIMIT 50
      |""".stripMargin).show

}
// end::full-example[]
