/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.table

import org.opencypher.caps.test.BaseTestSuite

class ColumnNameTest extends BaseTestSuite {

  test("escape length 0 spark identifier") {
    fromString("") should equal("_empty_")
  }

  test("escape length 1 spark identifiers") {
    fromString("a") should equal("a")
    fromString("1") should equal("_1")
    fromString("_") should equal("_bar_")
  }

  test("escape length > 1 spark identifiers") {
    fromString("aa") should equal("aa")
    fromString("a1") should equal("a1")
    fromString("_1") should equal("_bar_1")
    fromString("a_") should equal("a_bar_")
  }

  test("escape weird chars") {
    ".?!'\"`=@#$()^&%[]{}<>,:;|+*/\\-".foreach { ch =>
      fromString(s"$ch").forall(esc => Character.isLetter(esc) || esc == '_')
      fromString(s"a$ch").forall(esc => Character.isLetter(esc) || esc == '_')
      fromString(s"1$ch").forall(esc => Character.isLetterOrDigit(esc) || esc == '_')
      fromString(s"_$ch").forall(esc => Character.isLetter(esc) || esc == '_')
    }
  }

  def fromString(text: String) = ColumnName.from(Some(text))
}
