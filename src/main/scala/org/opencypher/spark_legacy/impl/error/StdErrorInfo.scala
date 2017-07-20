/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
