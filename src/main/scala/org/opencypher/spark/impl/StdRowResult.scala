package org.opencypher.spark.impl

import org.apache.spark.sql._
import org.opencypher.spark.CypherValue
import org.opencypher.spark.api.CypherResult
import org.opencypher.spark.impl.frame.{ProductAsMap, ProductAsRow, RowAsProduct}

class StdRowResult(val frame: StdCypherFrame[Row])(implicit val context: StdRuntimeContext) extends CypherResult[Row] {

  override def toDS: Dataset[Row] = toDF

  override def toDF: DataFrame = {
    val out = frame.run
    out
  }

  override def show(): Unit = {
    toDF.show(false)
  }
}

class StdProductResult(val frame: StdCypherFrame[Product])(implicit val context: StdRuntimeContext) extends CypherResult[Product] {

  override def toDS: Dataset[Product] = {
    val out = frame.run
    out
  }

  override def toDF: DataFrame = {
    val out = ProductAsRow(frame).run
    out
  }

  override def show(): Unit = {
    toDS.show(false)
  }
}


class StdMapResult(productFrame: StdCypherFrame[Product])(implicit val context: StdRuntimeContext) extends CypherResult[Map[String, CypherValue]] {

  override lazy val frame: StdCypherFrame[Map[String, CypherValue]] = {
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

  override def show(): Unit = {
    toDS.show(false)
  }
}
