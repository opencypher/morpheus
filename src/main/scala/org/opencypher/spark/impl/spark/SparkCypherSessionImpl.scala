package org.opencypher.spark.impl.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.record.{OpaqueField, RecordHeader}
import org.opencypher.spark.api.spark.{SparkCypherRecords, SparkCypherSession}

final class SparkCypherSessionImpl(val sparkSession: SparkSession) extends SparkCypherSession {

  override def importDataFrame(df: DataFrame): SparkCypherRecords =
    if (df.sparkSession == sparkSession) {
      new SparkCypherRecords {
        override val data: DataFrame = df
        override val header: RecordHeader =
          RecordHeader.from(df.schema.fields.map { field =>
            OpaqueField(Var(field.name)(fromSparkType(field.dataType, field.nullable)))
          }: _*)
      }
    } else
      throw new IllegalArgumentException("Importing of data frames created in another session is not supported")
}
