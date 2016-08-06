package org.opencypher.spark

import ammonite.util.Bind
import Bind._
import ammonite.util.Res.Success

import org.apache.spark.sql.SparkSession

object Shell {

  def main(args: Array[String]): Unit = {
    implicit val session = SparkSession.builder().master("local[4]").getOrCreate()
    val factory = PropertyGraphFactory.create
    try {
      val repl = new ammonite.Main(
        predef =
          s"""|repl.frontEnd() = ammonite.frontend.FrontEnd.JLineUnix
              |repl.prompt() = \"(cypher)-[:on]->(spark) \"
              |import org.opencypher.spark
              |import org.opencypher.spark._
              |import CypherValue.implicits
              |import CypherValue.implicits._
              |import CypherTypes._
              |import EntityData._
              |""".stripMargin
      ).instantiateRepl(Seq(
        "session" -> session,
        "factory" -> factory
      ))
      repl.run()
    } finally {
      session.stop()
    }
  }
}
