package org.opencypher.spark.legacy.impl

import org.apache.spark.sql._
import org.opencypher.spark.legacy.api.CypherResult
import org.opencypher.spark.legacy.impl.frame.{ProductAsMap, ProductAsRow}
import org.opencypher.spark.prototype.api.value.CypherValue

class StdRowResult(frame: StdCypherFrame[Row])(implicit val context: StdRuntimeContext) extends CypherResult[Row] {

  def signature = frame.signature

  override def toDS: Dataset[Row] = toDF

  override def toDF: DataFrame = {
    val out = frame.run
    out
  }
}

class StdProductResult(frame: StdCypherFrame[Product])(implicit val context: StdRuntimeContext) extends CypherResult[Product] {

  def signature = frame.signature

  override def toDS: Dataset[Product] = {
    val out = frame.run
    out
  }

  override def toDF: DataFrame = {
    val out = ProductAsRow(frame).run
    out
  }
}


class StdRecordResult(productFrame: StdCypherFrame[Product])(implicit val context: StdRuntimeContext) extends CypherResult[Map[String, CypherValue]] {

  def signature = frame.signature

  private lazy val frame: StdCypherFrame[Map[String, CypherValue]] = {
    ProductAsMap(productFrame)
  }

  override def toDS: Dataset[Map[String, CypherValue]] = {
    val out = frame.run
    out
  }

  override def toDF: DataFrame = {
    val out = ProductAsRow(productFrame).run
    out
  }
}
