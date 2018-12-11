package org.opencypher.memcypher

import org.opencypher.memcypher.api.MemCypherSession
import org.opencypher.memcypher.impl.table.{Row, Schema, Table}
import org.opencypher.memcypher.impl.cyphertable.{MemNodeTable, MemRelationshipTable}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.types.{CTInteger, CTString}
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintRelationalPlan

object Demo extends App {

  implicit val session: MemCypherSession = MemCypherSession()

  val graph = session.readFrom(DemoData.nodes, DemoData.rels)
  PrintRelationalPlan.set()
//  graph.cypher("MATCH (n)-->(m) WHERE n.age > 23 OR n.name = 'Alice' RETURN n, labels(n), m").show
//  graph.cypher("MATCH (n) RETURN n, n.name CONTAINS 'A' ORDER BY n.age ASC, n.name DESC").show
  graph.cypher("MATCH (n) RETURN n.gender, count(DISTINCT n.name), min(n.age), max(n.age), avg(n.age)").show
  graph.cypher("MATCH (n) RETURN count(*)").show

}

object DemoData {

  def nodes: MemNodeTable = {
    val schema = Schema.empty
      .withColumn("id", CTInteger)
      .withColumn("age", CTInteger)
      .withColumn("name", CTString)
      .withColumn("gender", CTString)

    val data = Seq(
      Row(0L, 23L, "Alice", "w"),
      Row(1L, 42L, "Bob", "m"),
      Row(2L, 42L, "Eve", "w"),
      Row(3L, 84L, "Frank", "m")
    )

    val nodeMapping = NodeMapping.on("id")
      .withImpliedLabel("Person")
      .withImpliedLabel("Human")
      .withPropertyKey("age")
      .withPropertyKey("name")
      .withPropertyKey("gender")

    MemNodeTable(nodeMapping, Table(schema, data))
  }

  def rels: MemRelationshipTable = {
    val schema = Schema.empty
      .withColumn("id", CTInteger)
      .withColumn("source", CTInteger)
      .withColumn("target", CTInteger)
      .withColumn("since", CTString)

    val data = Seq(Row(0L, 0L, 1L, 1984L))

    val relMapping = RelationshipMapping.on("id")
      .from("source")
      .to("target")
      .withRelType("KNOWS")
      .withPropertyKey("since")

    MemRelationshipTable(relMapping, Table(schema, data))
  }
}
