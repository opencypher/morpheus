package org.opencypher.spark.benchmark

object Neo4j {

  val nodeScanIdsSorted = """MATCH (n)
                            |WITH n
                            |  SKIP 0
                            |WITH n
                            |  LIMIT 100000
                            |WITH id(n) AS id
                            |WHERE n:Employee
                            |RETURN id, rand() AS r
                            |  ORDER BY r DESC""".stripMargin

}
