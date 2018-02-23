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
package org.opencypher.caps.api.configuration

import org.opencypher.caps.impl.configuration.ConfigFlag

object Configuration {

  /**
    * If enabled, the time required for executing a code block wrapped in
    * [[org.opencypher.caps.impl.util.Measurement.time()]] will be printed.
    */
  object PrintTimings extends ConfigFlag("caps.printTimings")

}
