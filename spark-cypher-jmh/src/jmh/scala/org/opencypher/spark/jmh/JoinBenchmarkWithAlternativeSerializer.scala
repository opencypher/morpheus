/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.jmh

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.openjdk.jmh.annotations._

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

object CustomByteArraySerializer extends Serializer[Array[Byte]] {

  private final val moreBytesForSameIntBitMask = Integer.parseInt("10000000", 2)
  private final val varInt7BitMask = Integer.parseInt("01111111", 2)
  private final val otherBitsMask = ~varInt7BitMask


  override final def write(kryo: Kryo, output: Output, a: Array[Byte]): Unit = {
    write128Varint(a.length, output)
    output.writeBytes(a)
  }

  override def read(kryo: Kryo, input: Input, t: Class[Array[Byte]]): Array[Byte] = {
    val aLength = read128Varint(input)
    input.readBytes(aLength)
  }

  // Same encoding as as Base 128 Varints @ https://developers.google.com/protocol-buffers/docs/encoding
  @inline
  private final def write128Varint(i: Integer, output: Output): Unit = {
    var remainder = i
    while ((remainder & otherBitsMask) != 0) {
      output.writeByte((remainder & varInt7BitMask) | moreBytesForSameIntBitMask)
      remainder >>>= 7
    }
    output.writeByte(remainder)
  }

  // Same encoding as as Base 128 Varints @ https://developers.google.com/protocol-buffers/docs/encoding
  @inline
  private final def read128Varint(input: Input): Int = {
    var currentByte = input.readByte
    var decoded = currentByte & varInt7BitMask
    var nextLeftShift = 7
    while ((currentByte & moreBytesForSameIntBitMask) != 0) {
      currentByte = input.readByte
      decoded |= (currentByte & varInt7BitMask) << nextLeftShift
      nextLeftShift += 7
    }
    decoded
  }

}

class ByteArrayRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Array[Byte]], CustomByteArraySerializer)
  }
}

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
class JoinBenchmarkWithAlternativeSerializer {

  implicit var sparkSession: SparkSession = _

  val idColumn = "id"

  var leftData: List[Long] = _
  var rightData: List[Long] = _

  var leftLong: DataFrame = _
  var rightLong: DataFrame = _

  var leftByteArray: DataFrame = _
  var rightByteArray: DataFrame = _

  @Setup
  def setUp(): Unit = {
    sparkSession = SparkSession.builder()
      .config("spark.kryo.registrator", "org.opencypher.spark.jmh.ByteArrayRegistrator")
      .master("local[*]")
      .getOrCreate()

    val rangeStart = 1000000000L
    val joinCount = 10000
    val leftRandomCount = 100000
    val rightRandomCount = 10000
    val joinRange = Range(0, joinCount).map(_.toLong + rangeStart)

    leftData = List.fill(leftRandomCount)(Random.nextLong()) ++ joinRange
    rightData = List.fill(rightRandomCount)(Random.nextLong()) ++ joinRange

    def prepareDf[A: TypeTag](data: List[A]): DataFrame = {
      sparkSession.createDataFrame(data.map(Tuple1(_))).toDF(idColumn)
    }

    leftLong = prepareDf(leftData).partitionAndCache
    rightLong = prepareDf(rightData).partitionAndCache

    leftByteArray = prepareDf(leftData.map(longToByteArray)).partitionAndCache
    rightByteArray = prepareDf(rightData.map(longToByteArray)).partitionAndCache
  }

  @Benchmark
  def joinLongIds(): Long = leftLong.join(rightLong, idColumn).count()

  @Benchmark
  def joinByteArrayIds(): Long = leftByteArray.join(rightByteArray, idColumn).count()

  implicit class DataFrameSetup(df: DataFrame) {

    def partitionAndCache: DataFrame = {
      val cached = df.repartition(10).persist(StorageLevel.MEMORY_ONLY)
      cached.count()
      cached
    }

    def castIdToString: DataFrame = df.select(df.col(idColumn).cast(StringType).as(idColumn))
  }

  private def longToByteArray(l: Long): Array[Byte] = {
    val a = new Array[Byte](8)
    a(0) = (l & 0xFF).asInstanceOf[Byte]
    a(1) = ((l >> 8) & 0xFF).asInstanceOf[Byte]
    a(2) = ((l >> 16) & 0xFF).asInstanceOf[Byte]
    a(3) = ((l >> 24) & 0xFF).asInstanceOf[Byte]
    a(4) = ((l >> 32) & 0xFF).asInstanceOf[Byte]
    a(5) = ((l >> 40) & 0xFF).asInstanceOf[Byte]
    a(6) = ((l >> 48) & 0xFF).asInstanceOf[Byte]
    a(7) = ((l >> 56) & 0xFF).asInstanceOf[Byte]
    a
  }
}
