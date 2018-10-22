package org.opencypher.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.StructType

import scala.tools.nsc.interpreter.JList

class AdvancedDataSourceV2 extends DataSourceV2 with ReadSupport {

  class Reader(rowCount: Int) extends DataSourceReader
    with SupportsPushDownRequiredColumns {

    var requiredSchema = new StructType().add("i", "int").add("j", "int")

    override def pruneColumns(requiredSchema: StructType): Unit = {
      this.requiredSchema = requiredSchema
    }


    override def readSchema(): StructType = {
      requiredSchema
    }

    override def planInputPartitions(): JList[InputPartition[InternalRow]] = {
        val res = new java.util.ArrayList[InputPartition[InternalRow]]

        res.add(new AdvancedInputPartition(0, 5, requiredSchema))
        res.add(new AdvancedInputPartition(5, rowCount, requiredSchema))

        res
    }
  }

  override def createReader(options: DataSourceOptions): DataSourceReader =
    new Reader(options.get("rows").orElse("10").toInt)
}

class AdvancedInputPartition(start: Int, end: Int, requiredSchema: StructType)
  extends InputPartition[InternalRow] with InputPartitionReader[InternalRow] {

  private var current = start - 1

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new AdvancedInputPartition(start, end, requiredSchema)
  }

  override def close(): Unit = {}

  override def next(): Boolean = {
    current += 1
    current < end
  }

  override def get(): InternalRow = {
    val values = requiredSchema.map(_.name).map {
      case "i" => current
      case "j" => -current
    }
    InternalRow.fromSeq(values)
  }
}

object DataSourceTest extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val cls = classOf[AdvancedDataSourceV2]

  val with100 = spark.read.format(cls.getName).option("rows", 100).load()
  val with10 = spark.read.format(cls.getName).option("rows", 10).load()


  assert(with100.union(with10).count == 110)
}
