package org.opencypher.spark.api.spark

import java.io.PrintStream

import org.opencypher.spark.impl.spark.SparkColumnName

object RecordsPrinter {

  /**
    * Prints the given SparkCypherRecords to stdout
    * @param records the records to be printed.
    */
  def print(records: SparkCypherRecords): Unit = {
    print(records, Console.out)
  }

  def print(records: SparkCypherRecords, stream: PrintStream): Unit = {
    val fieldContents = records.header.slots.map(_.content)
    val lineWidth = 20 * fieldContents.size + 5
    val --- = "+" + repeat("-", lineWidth) + "+"

    stream.println(---)
    var sep = "| "
    fieldContents.foreach { contents =>
      stream.print(sep)
      stream.print(fitTo20(contents.key.withoutType))
      sep = " | "
    }
    stream.println(" |")
    stream.println(---)
    records.toScalaIterator.foreach { map =>
      sep = "| "
      fieldContents.foreach { content =>
        map.get(SparkColumnName.of(content)) match {
          case None =>
            stream.print(sep)
            stream.print(fitTo20("null"))
            sep = " | "
          case Some(v) =>
            stream.print(sep)
            stream.print(fitTo20(v.toString))
            sep = " | "
        }
      }
      stream.println(" |")
    }
    stream.println(---)
    stream.flush()
  }

  private def repeat(x: String, size: Int): String = (1 to size).map((_) => x).mkString
  private def fitTo20(s: String) = (s + "                    ").take(20)

}
