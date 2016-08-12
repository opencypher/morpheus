package org.opencypher.spark.impl

import org.apache.spark.sql._
import org.opencypher.spark.api.{CypherResult, CypherValue}
import org.opencypher.spark.impl.frame.{ProductAsMap, ProductAsRow, RowAsProduct}

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
