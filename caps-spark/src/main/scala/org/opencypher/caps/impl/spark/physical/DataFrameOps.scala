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
package org.opencypher.caps.impl.spark.physical

import org.apache.spark.sql.{Column, DataFrame}

object DataFrameOps {
  implicit class RichDataFrame(data: DataFrame) {
    /**
      * TODO clean up
      */
    def safeAddColumn(name: String, col: Column):DataFrame = {
      require(!data.columns.contains(name))
      data.withColumn(name, col)
    }

    /**
      * TODO clean up
      */
    def safeRenameColumn(oldName: String, newName: String): DataFrame = {
      require(!data.columns.contains(newName))
      data.withColumnRenamed(oldName, newName)
    }

    def safeDropColumn(colName: String): DataFrame = {
       data.drop(colName)
    }

    def safeDropColumns(colNames: String*): DataFrame = {
      data.drop(colNames: _*)
    }

    def safeJoin(other: DataFrame, joinCols: Seq[(String, String)], joinType: String): DataFrame = {
      require(joinCols.map(_._1).forall(col => !other.columns.contains(col)))
      require(joinCols.map(_._2).forall(col => !data.columns.contains(col)))

      val joinExpr = joinCols.map{
        case (l, r) => data.col(l) === other.col(r)
      }.reduce(_ && _)

      data.join(other, joinExpr, joinType)
    }
  }
}
