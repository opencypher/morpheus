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
package org.opencypher.caps.impl.spark.io.session

import java.net.URI

import org.mockito.Mockito._
import org.opencypher.caps.api.spark.io.CAPSGraphSource
import org.opencypher.caps.test.BaseTestSuite

class SessionGraphSourceFactoryTest extends BaseTestSuite {

  test("mounting graphs should be thread-safe") {
    val f = SessionGraphSourceFactory()
    (0 until 1000).par.foreach { i =>
      if (i % 2 == 0) {
        f.mountSourceAt(mock(classOf[CAPSGraphSource]), URI.create(s"session:/${i}"))(null)
      } else {
        f.mountPoints.get(URI.create(s"session:/${i + 1}").getPath)
      }
    }
    f.mountPoints.size should equal(500)
    (0 until 1000 by 2).forall { i =>
      val instance = f.mountPoints.get(URI.create(s"session:/${i}").getPath)
      instance != null
    } should equal(true)
  }

}
