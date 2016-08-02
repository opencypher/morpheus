package org.opencypher.spark.impl

import org.apache.spark.sql.Encoder
import org.opencypher.spark.CypherValue

object StdRecord {
  object implicits extends implicits

  trait implicits {
    implicit def valueRecordEncoder: Encoder[StdRecord] = org.apache.spark.sql.Encoders.kryo[StdRecord]
  }
}

case class StdRecord(values: Array[CypherValue], longs: Array[Long])



