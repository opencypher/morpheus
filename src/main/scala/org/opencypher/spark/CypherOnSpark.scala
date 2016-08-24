package org.opencypher.spark

import java.util.Properties

import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrator}

import scala.io.Source
import scala.language.postfixOps
import scala.util.Try


object CypherOnSpark {

  self =>

  val registrator = new CypherKryoRegistrar

  def version = {
    val clazz = self.getClass
    Try {
      // 1) Try to read from maven descriptor
      val inputStream = clazz.getResourceAsStream("/META-INF/maven/org.opencypher/cypher-on-spark/pom.properties")
      val properties = new Properties()
      properties.load(inputStream)
      value(properties.getProperty("version"))
    } orElse Try {
      // 2) Use clazz package implementation version
      value(clazz.getPackage.getImplementationVersion)
    } orElse Try {
      // 3) Use clazz package specification version as a fallback
      value(clazz.getPackage.getSpecificationVersion)
    } orElse Try {
      // 4) We're likely running in some development setup; attempt to read from guessed location of version.txt
      value[String](Source.fromFile("target/classes/version.txt", "UTF-8").mkString)
    } orElse Try {
      // 5) We're likely running in some development setup; attempt to read from other guessed location of version.txt
      value[String](Source.fromFile("version.txt", "UTF-8").mkString)
    } orElse Try {
      // 6) Nothing worked? Hopefully someone set us a property to report
      value(System.getProperty("project.version"))
    } toOption
  }

  private def value[T](v: T) = Option(v).getOrElse(throw new NullPointerException)
}
