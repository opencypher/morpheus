package org.opencypher.caps.test.fixture

import org.apache.spark.sql.Row

trait TeamDataFixture extends TestDataFixture {

  override val dataFixture =
    """
       CREATE (a:Person:German {name: "Stefan", luckyNumber: 42})
       CREATE (b:Person:Swede  {name: "Mats", luckyNumber: 23})
       CREATE (c:Person:German {name: "Martin", luckyNumber: 1337})
       CREATE (d:Person:German {name: "Max", luckyNumber: 8})
       CREATE (a)-[:KNOWS {since: 2016}]->(b)
       CREATE (b)-[:KNOWS {since: 2016}]->(c)
       CREATE (c)-[:KNOWS {since: 2016}]->(d)
    """

  override def nbrNodes = 4

  override def nbrRels = 3

  def teamDataGraphNodes: Set[Row] = Set(
    Row(0, true, true, false, 42, "Stefan"),
    Row(1, true, false, true, 23, "Mats"),
    Row(3, true, true, false, 8, "Max"),
    Row(2, true, true, false, 1337, "Martin")
  )

  def teamDataGraphRels: Set[Row] = Set(
    Row(0, 0, 0, 1, 2016),
    Row(1, 1, 0, 2, 2016),
    Row(2, 2, 0, 3, 2016)
  )
}
