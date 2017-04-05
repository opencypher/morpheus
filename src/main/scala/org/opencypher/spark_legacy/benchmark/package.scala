package org.opencypher.spark_legacy

import org.apache.spark.sql.DataFrame

package object benchmark {

  object countAndChecksum extends (DataFrame => (Long, Int)) {
    override def apply(frame: DataFrame): (Long, Int) = {
      frame.rdd.treeAggregate((0l, 0))({
        case ((c, cs), row) =>
          val checksum = (0 until row.length).map(row.get(_).hashCode).foldLeft(0)(_ ^ _)
          (c + 1l, cs ^ checksum)
      }, {
        case ((lCount, lChecksum), (rCount, rChecksum)) => (lCount + rCount, lChecksum ^ rChecksum)
      })
    }
  }
}
