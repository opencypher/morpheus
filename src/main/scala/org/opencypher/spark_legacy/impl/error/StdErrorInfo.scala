package org.opencypher.spark_legacy.impl.error

import scala.reflect.io.Path
import scala.util.Try

object StdErrorInfo {

  trait Implicits {
    implicit def cypherErrorContext(implicit fullName: sourcecode.FullName,
                                    name: sourcecode.Name,
                                    enclosing: sourcecode.Enclosing,
                                    pkg: sourcecode.Pkg,
                                    file: sourcecode.File,
                                    line: sourcecode.Line) =
      StdErrorInfo(fullName, name, enclosing, pkg, file, line)
  }

  object Implicits extends Implicits
}

final case class StdErrorInfo(fullName: sourcecode.FullName,
                              name: sourcecode.Name,
                              enclosing: sourcecode.Enclosing,
                              pkg: sourcecode.Pkg,
                              file: sourcecode.File,
                              line: sourcecode.Line) {

  def message(detail: String) = s"[${name.value}] $detail ($locationText)"

  def location = file.value -> line.value

  def locationText = {
    val relativeFileName = Try { Path(file.value).name }.getOrElse(file.value)
    s"$relativeFileName:${line.value}"
  }
}
