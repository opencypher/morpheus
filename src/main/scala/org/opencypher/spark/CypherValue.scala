package org.opencypher.spark

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

sealed trait CypherValue

final case class CypherMap(v: Map[String, CypherValue]) extends CypherValue

object CypherMap {
  val empty = CypherMap(Map.empty)
}

final case class CypherNode(id: Long, labels: Array[String], properties: CypherMap) extends CypherValue

object CypherNode {

  var _idCounter = new AtomicLong(0L)

  def apply(): CypherNode = new CypherNode(_idCounter.incrementAndGet(), Array.empty[String], CypherMap.empty)
  def apply(pair: (String, CypherString)): CypherNode = new CypherNode(_idCounter.incrementAndGet(), Array.empty[String], CypherMap(Map(pair)))
  def apply(labels: String*): CypherNode = new CypherNode(_idCounter.incrementAndGet(), labels.toArray[String], CypherMap.empty)
}

final case class CypherRelationship(id: Long, start: Long, end: Long, typ: String, properties: CypherMap) extends CypherValue
object CypherRelationship {

  var _idCounter = new AtomicLong(0L)

  def apply(start: Long, end: Long, typ: String): CypherRelationship = new CypherRelationship(_idCounter.incrementAndGet(), start, end, typ, CypherMap.empty)
}

final case class CypherString(v: String) extends CypherValue

final case class CypherInteger(v: Long) extends CypherValue

case class CypherRecord(values: Array[CypherValue], ids: Array[Long])

trait CypherEncoders {
  implicit def cypherValueEncoder[T <: CypherValue : ClassTag]: Encoder[T] = org.apache.spark.sql.Encoders.kryo[T]
  implicit def cypherRecordEncoder: Encoder[CypherRecord] = org.apache.spark.sql.Encoders.kryo[CypherRecord]
}
