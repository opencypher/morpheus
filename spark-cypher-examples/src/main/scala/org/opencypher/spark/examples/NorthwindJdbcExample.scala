// tag::full-example[]
package org.opencypher.spark.examples

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.{ConsoleApp, NorthwindDB}

object NorthwindJdbcExample extends ConsoleApp {

  implicit val resourceFolder: String = "/northwind"

  // start up the SQL database
  NorthwindDB.init()

  // Initialise local CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // define the root configuration directory for the SQL graph source
  // this holds the data source mappings files and the SQL DDL file
  // the latter contains the graph definitions and mappings from SQL tables that fill the graph with data

  val sqlGraphSource = GraphSources
    .sql(resource("ddl/northwind.ddl").getFile)
    .withSqlDataSourceConfigs(resource("ddl/jdbc-data-sources.json").getFile)

  // register the SQL graph source with the session
  session.registerSource(Namespace("sql"), sqlGraphSource)


  // print the number of nodes in the graph
  session.cypher(
    """
      |FROM GRAPH sql.Northwind
      |MATCH (n)
      |RETURN count(n)
    """.stripMargin).records.show

  // print the schema of the graph
  println(session.catalog.graph("sql.Northwind").schema.pretty)

  // run a simple query
  session.cypher(
    """
      |FROM GRAPH sql.Northwind
      |MATCH (e:Employee)-[:REPORTS_TO]->(:Employee)<-[:HAS_EMPLOYEE]-(o:Order)
      |RETURN o.customerID AS customer, o.orderDate AS orderedAt, e.lastName AS handledBy, e.title AS employee
      |  ORDER BY o.orderDate, handledBy, customer
      |  LIMIT 50
      |""".stripMargin).show

}
// end::full-example[]
