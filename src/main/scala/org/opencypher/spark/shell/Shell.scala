package org.opencypher.spark.shell

import ammonite.util.Bind._
import ammonite.util.Util
import org.apache.spark.sql.SparkSession
import org.opencypher.spark.{CypherOnSpark, PropertyGraphFactory}

object Shell {

  def main(args: Array[String]): Unit = {
    implicit val session = SparkSession.builder().master("local[4]").getOrCreate()
    try {
      val welcomeBanner = {
        val ownVersion = CypherOnSpark.version.getOrElse("<unknown>")
        val ammoniteVersion = ammonite.Constants.version
        val scalaVersion = scala.util.Properties.versionNumberString
        val javaVersion = System.getProperty("java.version")
        val sparkVersion = session.version
        Util.normalizeNewlines(
          """=
              = open      |                                     |
              =/~~\  /|~~\|/~\ /~/|/~\  /~\|/~\   (~|~~\/~~||/~\|_/
              =\__ \/ |__/|   |\/_|     \_/|   |  _)|__/\__||   | \
              =   _/  |                             |
              =""".stripMargin('=') +
          s"""|
              |Version $ownVersion
              |(Apache Spark $sparkVersion, Scala $scalaVersion, Java $javaVersion, Ammonite $ammoniteVersion)
              |
              |Cypher is a registered trademark of Neo Technology, Inc.
              |
           """.stripMargin
        )
      }
      val frontend = if (System.getProperty("os.name").startsWith("Windows")) "JLineWindows" else "JLineUnix"

      val repl = new ammonite.Main(
        welcomeBanner = Some(welcomeBanner),
        predef =
          s"""|repl.frontEnd() = ammonite.frontend.FrontEnd.$frontend
              |repl.prompt() = \"(cypher)-[:on]->(spark) \"
              |import org.opencypher.spark
              |import org.opencypher.spark._
              |import org.opencypher.spark.api._
              |import org.opencypher.spark.api.implicits._
              |import org.opencypher.spark.api.types._
              |""".stripMargin
      ).instantiateRepl(Seq(
        "session" -> session,
        "factory" -> PropertyGraphFactory.create
      ))
      repl.run()
    } finally {
      session.stop()
    }

    // Needed; otherwise the shell hangs on exit
    System.exit(0)
  }
}
