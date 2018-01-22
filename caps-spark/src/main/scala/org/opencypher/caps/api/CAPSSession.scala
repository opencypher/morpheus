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
package org.opencypher.caps.api

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.caps.api.graph.CypherSession
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSResult, CAPSSessionImpl}
import org.opencypher.caps.api.spark.io.CAPSPropertyGraphDataSourceFactory
import org.opencypher.caps.demo.CypherKryoRegistrar
import org.opencypher.caps.impl.spark.io.CAPSGraphSourceHandler
import org.opencypher.caps.impl.spark.io.file.FileCsvPropertyGraphDataSourceFactory
import org.opencypher.caps.impl.spark.io.hdfs.HdfsCsvPropertyGraphDataSourceFactory
import org.opencypher.caps.impl.spark.io.neo4j.Neo4JPropertyGraphDataSourceFactory
import org.opencypher.caps.impl.spark.io.session.SessionPropertyGraphDataSourceFactory

trait CAPSSession extends CypherSession {

  def sparkSession: SparkSession
}

object CAPSSession extends Serializable {

  /**
    * Creates a new CAPSSession that wraps a local Spark session with CAPS default parameters.
    */
  def local(): CAPSSession = {
    val conf = new SparkConf(true)
    conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    conf.set("spark.kryo.registrator", classOf[CypherKryoRegistrar].getCanonicalName)
    conf.set("spark.sql.codegen.wholeStage", "true")
    conf.set("spark.kryo.unsafe", "true")
    conf.set("spark.kryo.referenceTracking", "false")
    conf.set("spark.kryo.registrationRequired", "true")

    val session = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()
    session.sparkContext.setLogLevel("error")

    create(session)
  }

  def create(implicit session: SparkSession): CAPSSession = Builder(session).build

  case class Builder(session: SparkSession, private val graphSourceFactories: Set[CAPSPropertyGraphDataSourceFactory] = Set.empty) {

    def withGraphSourceFactory(factory: CAPSPropertyGraphDataSourceFactory): Builder =
      copy(graphSourceFactories = graphSourceFactories + factory)

    def build: CAPSSession = {
      val sessionFactory = SessionPropertyGraphDataSourceFactory()
      // add some default factories
      val additionalFactories = graphSourceFactories +
        Neo4JPropertyGraphDataSourceFactory() +
        HdfsCsvPropertyGraphDataSourceFactory(session.sparkContext.hadoopConfiguration) +
        FileCsvPropertyGraphDataSourceFactory()

      new CAPSSessionImpl(
        session,
        CAPSGraphSourceHandler(sessionFactory, additionalFactories)
      )
    }
  }

  def builder(sparkSession: SparkSession): Builder = Builder(sparkSession)
}
