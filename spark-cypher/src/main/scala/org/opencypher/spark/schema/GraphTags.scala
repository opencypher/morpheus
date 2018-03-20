package org.opencypher.spark.schema

import org.apache.spark.sql.SparkSession

object GraphTags extends App {

  val nodeId1 = 1
  val nodeId2 = 2

  val totalBits = 64
  val idBits = 50

  val tagMask: Long = -1L << idBits

  def setTag(nodeId: Long, tag: Int): Long = {
    require((nodeId & tagMask) == 0)
    val r = nodeId | (tag << idBits)
    println(s"setTag(nodeId=$nodeId, tag=$tag)=$r")
    r
  }

  def getTag(nodeId: Long): Long = {
    val r = (nodeId & tagMask) >> idBits
    println(s"getTag(nodeId=$nodeId)=$r")
    r
  }

    


  getTag(setTag(1, 0))
  getTag(setTag(1, 1))
  getTag(setTag(2, 0))
  getTag(setTag(2, 1))



}
