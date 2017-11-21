/*
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
package org.opencypher.caps.impl.common

import scala.util.hashing.MurmurHash3

/**
  * Allows to exclude constructor parameters from inclusion in the `hashCode` and `equals`
  * methods of a product.
  */
trait ModifiedProduct extends Product {

  protected def excludeFromComparisons(p: Any): Boolean

  protected def includedParameters = productIterator.filter(!excludeFromComparisons(_))

  override def hashCode(): Int = {
    MurmurHash3.orderedHash(includedParameters, MurmurHash3.stringHash(productPrefix))
  }

  override def equals(other: Any): Boolean = {
    other != null && other.isInstanceOf[ModifiedProduct] && {
      val castOther = other.asInstanceOf[ModifiedProduct]
      this.eq(castOther) || {
        productPrefix == castOther.productPrefix &&
          haveEqualValues(includedParameters, castOther.includedParameters)
      }
    }
  }

  protected def haveEqualValues(a: Iterator[Any], b: Iterator[Any]): Boolean = {
    while (a.hasNext && b.hasNext) {
      if (a.next != b.next) return false
    }
    a.hasNext == b.hasNext
  }

}
