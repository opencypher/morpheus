package org.opencypher.spark

import ammonite.util.Bind._
import ammonite.util.Util
import org.apache.spark.sql.SparkSession

object Shell {


  def main(args: Array[String]): Unit = {
    implicit val session = SparkSession.builder().master("local[4]").getOrCreate()
    try {
      val welcomeBanner = {
        val ownVersion = getClass.getPackage.getImplementationVersion
        val ownVersionString = if (ownVersion == null) "Version unknown" else s"Version $ownVersion"
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
              |$ownVersionString
              |(Spark $sparkVersion Scala $scalaVersion Java $javaVersion Ammonite $ammoniteVersion)
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
              |import CypherValue.implicits
              |import CypherValue.implicits._
              |import CypherTypes._
              |import EntityData._
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
