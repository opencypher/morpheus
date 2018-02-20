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
package org.opencypher.caps.cosc.impl.planning

import org.opencypher.caps.api.io.QualifiedGraphName
import org.opencypher.caps.api.physical.PhysicalOperator
import org.opencypher.caps.cosc.impl.COSCConverters._
import org.opencypher.caps.cosc.impl.{COSCGraph, COSCPhysicalResult, COSCRecords, COSCRuntimeContext}
import org.opencypher.caps.impl.exception.IllegalArgumentException
import org.opencypher.caps.trees.AbstractTreeNode

abstract class COSCOperator extends AbstractTreeNode[COSCOperator]
  with PhysicalOperator[COSCRecords, COSCGraph, COSCRuntimeContext] {

  def execute(implicit context: COSCRuntimeContext): COSCPhysicalResult

  protected def resolve(qualifiedGraphName: QualifiedGraphName)(implicit context: COSCRuntimeContext): COSCGraph = {
    context.resolve(qualifiedGraphName).map(_.asCosc).getOrElse(throw IllegalArgumentException(s"a graph at $qualifiedGraphName"))
  }
}

