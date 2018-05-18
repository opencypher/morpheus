/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.api.io.util

import scala.annotation.tailrec

object StringEncodingUtilities {

  val propertyPrefix: String = "property#"

  // Hex string length is up to 4 characters
  protected val maxCharactersInHexStringEncoding: Int = 4
  protected val totalEscapeSequenceLength: Int = maxCharactersInHexStringEncoding + 1

  implicit class CharOps(val c: Char) extends AnyVal {
    def isAscii: Boolean = c.toInt <= 127
  }

  implicit class StringOps(val s: String) extends AnyVal {

    def toPropertyColumnName: String = {
      s"$propertyPrefix${s.encodeSpecialCharacters}"
    }

    def isPropertyColumnName: Boolean = s.startsWith(propertyPrefix)

    def toProperty: String = {
      if (s.isPropertyColumnName) {
        s.drop(propertyPrefix.length).decodeSpecialCharacters
      } else {
        s
      }
    }

    /**
      * Encodes special characters in a string.
      *
      * The encoded string contains only ASCII letters, numbers, '_', '#', and '@'. The encoded string is compatible
      * with both SQL column names and file paths.
      *
      * @return encoded string
      */
    def encodeSpecialCharacters: String = {
      val sb = new StringBuilder
      @tailrec def recEncode(remaining: List[Char]): Unit = {
        if (remaining.nonEmpty) {
          val head :: tail = remaining
          if ((head.isLetterOrDigit && head.isAscii) || head == '_' || head == '#') {
            sb.append(head)
          } else {
            sb.append("@")
            val hexString = head.toHexString
            // Pad left to max encoded length with '0's
            for (_ <- 0 until maxCharactersInHexStringEncoding - hexString.length) sb.append('0')
            sb.append(hexString)
          }
          recEncode(tail)
        }
      }
      recEncode(s.toList)
      sb.toString
    }

    /**
      * Recovers the original string from a string encoded with [[encodeSpecialCharacters]].
      *
      * @return original string
      */
    def decodeSpecialCharacters: String = {
      val sb = new StringBuilder
      @tailrec def recDecode(remaining: List[Char]): Unit = {
        if (remaining.nonEmpty) {
          remaining.head match {
            case '@' =>
              val hexString = remaining.tail.take(maxCharactersInHexStringEncoding).mkString
              sb.append(hexString.parseHex)
              recDecode(remaining.drop(totalEscapeSequenceLength))
            case other =>
              sb.append(other)
              recDecode(remaining.tail)
          }
        }
      }
      recDecode(s.toList)
      sb.toString
    }

    def parseHex: Char = Integer.parseInt(s, 16).toChar

  }

}
