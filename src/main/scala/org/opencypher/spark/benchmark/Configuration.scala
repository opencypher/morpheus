package org.opencypher.spark.benchmark

object Configuration {

  abstract class ConfigOption[T](val name: String, val defaultValue: T)(convert: String => Option[T]) {
    def get(): T = Option(System.getProperty(name)).flatMap(convert).getOrElse(defaultValue)

    override def toString: String = {
      val filled = name + (name.length to 25).map(_ => " ").reduce(_ + _)
      s"$filled = ${get()}"
    }
  }

  object GraphSize extends ConfigOption("cos.graph-size", -1l)(x => Some(java.lang.Long.parseLong(x)))
  object MasterAddress extends ConfigOption("cos.master", "local[*]")(Some(_))
  object Logging extends ConfigOption("cos.logging", "OFF")(Some(_))
  object Partitions extends ConfigOption("cos.shuffle-partitions", 40)(x => Some(java.lang.Integer.parseInt(x)))
  object Runs extends ConfigOption("cos.runs", 6)(x => Some(java.lang.Integer.parseInt(x)))
  object WarmUpRuns extends ConfigOption("cos.warmupRuns", 2)(x => Some(java.lang.Integer.parseInt(x)))
  object NodeFilePath extends ConfigOption("cos.nodeFile", "<>")(Some(_))
  object RelFilePath extends ConfigOption("cos.relFile", "<>")(Some(_))
  object Neo4jPassword extends ConfigOption("cos.neo4j-pw", ".")(Some(_))
  object Benchmarks extends ConfigOption("cos.benchmarks", "frames")(Some(_))
  object Query extends ConfigOption("cos.query", 5)(x => Some(java.lang.Integer.parseInt(x)))

  val conf = Seq(GraphSize, MasterAddress, Logging, Partitions, Runs, WarmUpRuns, NodeFilePath,
    RelFilePath, Neo4jPassword, Benchmarks, Query)

  def print(): Unit = {
    conf.foreach(println)
  }

}
