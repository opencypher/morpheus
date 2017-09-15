package org.opencypher.caps.test.support

import org.apache.spark.sql.Row

object formatRowSet extends (Set[Row] => String) {
  def apply(rows: Set[Row]): String =
    rows.map(formatRow).mkString("Set(", ",\n", ")")
}
