package org.opencypher.spark.api.io.util

object FileSystemUtils {

  def using[T, U <: AutoCloseable](u: U)(f: U => T): T = {
    try {
      f(u)
    } finally {
      u.close()
    }
  }

}
