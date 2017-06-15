package org.opencypher.spark.impl.instances.spark

import java.util.Collections

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.opencypher.spark.api.expr.{Expr, Subtract, Var}
import org.opencypher.spark.api.record.{OpaqueField, ProjectedField, RecordHeader}
import org.opencypher.spark.impl.instances.spark.SparkSQLExprMapper.asSparkSQLExpr
import org.opencypher.spark.impl.physical.RuntimeContext
import org.opencypher.spark.impl.syntax.header.{addContents, _}
import org.opencypher.spark.{TestSparkCypherSession, TestSuiteImpl}

import scala.language.implicitConversions

class SparkSQLExprMapperTest extends TestSuiteImpl with TestSparkCypherSession.Fixture {

  test("can map subtract") {
    val expr = Subtract(Var("a")(), Var("b")())()

    convert(expr, _header.update(addContent(ProjectedField('foo, expr)))) should equal(Some(
      df.col("a") - df.col("b")
    ))
  }

  private def convert(expr: Expr, header: RecordHeader = _header): Option[Column] = {
    asSparkSQLExpr(header, expr, df)(RuntimeContext.empty)
  }

  val _header: RecordHeader = RecordHeader.empty.update(addContents(Seq(OpaqueField('a), OpaqueField('b))))
  val df: DataFrame = sparkSession.createDataFrame(Collections.emptyList[Row](),
    StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType))))

  implicit def extractRecordHeaderFromResult[T](tuple: (RecordHeader, T)): RecordHeader = tuple._1
}
