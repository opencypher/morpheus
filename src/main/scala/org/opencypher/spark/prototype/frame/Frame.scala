package org.opencypher.spark.prototype.frame

import org.apache.spark.sql.DataFrame

trait Frame {
  def header: Seq[Slot]

  def toDF: DataFrame
}
