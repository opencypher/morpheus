package org.opencypher.memcypher

import org.opencypher.memcypher.api.MemCypherSession
import org.opencypher.memcypher.impl.table.{Row, Table, Schema}
import org.opencypher.memcypher.impl.cyphertable.{MemNodeTable, MemRelationshipTable}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.types.{CTInteger, CTString}

object Demo extends App {

  implicit val session: MemCypherSession = MemCypherSession()

  val graph = session.readFrom(DemoData.nodes, DemoData.rels)

  graph.cypher("MATCH (n)-->(m) WHERE n.age > 23 OR n.name = 'Alice' RETURN n, m").show

}

object DemoData {

  def nodes: MemNodeTable = {
    val schema = Schema.empty
      .withColumn("id", CTInteger)
      .withColumn("age", CTInteger)
      .withColumn("name", CTString)

    val data = Seq(
      Row(0L, 23L, "Alice"),
      Row(1L, 42L, "Bob")
    )

    val nodeMapping = NodeMapping.on("id")
      .withImpliedLabel("Person")
      .withPropertyKey("age")
      .withPropertyKey("name")

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
