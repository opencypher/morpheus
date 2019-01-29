/**
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.api.io

import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.rules.TemporaryFolder
import org.opencypher.graphddl
import org.opencypher.graphddl.Graph
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.neo4j.io.MetaLabelSupport
import org.opencypher.okapi.neo4j.io.testing.Neo4jServerFixture
import org.opencypher.spark.api.FSGraphSources.FSGraphSourceFactory
import org.opencypher.spark.api.io.FileFormat._
import org.opencypher.spark.api.io.fs.DefaultGraphDirectoryStructure.nodeTableDirectoryName
import org.opencypher.spark.api.io.sql.IdGenerationStrategy._
import org.opencypher.spark.api.io.sql.SqlDataSourceConfig.Hive
import org.opencypher.spark.api.io.sql.util.DdlUtils._
import org.opencypher.spark.api.io.sql.{SqlDataSourceConfig, SqlPropertyGraphDataSource}
import org.opencypher.spark.api.io.util.CAPSGraphExport._
import org.opencypher.spark.api.{CypherGraphSources, GraphSources}
import org.opencypher.spark.impl.table.SparkTable._
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.api.io.CAPSCypher10AcceptanceTest
import org.opencypher.spark.testing.fixture.{H2Fixture, HiveFixture, MiniDFSClusterFixture}
import org.opencypher.spark.testing.utils.H2Utils._

import scala.collection.JavaConverters._

class FullCypher10AcceptanceTests extends CAPSTestSuite
  with CAPSCypher10AcceptanceTest with MiniDFSClusterFixture with Neo4jServerFixture with H2Fixture with HiveFixture {

  val fileFormatOptions = List(csv, parquet, orc)
  val filesPerTableOptions = List(1) //, 10
  val idGenerationOptions = List(HashBasedId, MonotonicallyIncreasingId)

  val fileSystemContextFactories: List[TestContextFactory] = {
    for {
      format <- fileFormatOptions
      filesPerTable <- filesPerTableOptions
    } yield List(
      new LocalFileSystemContextFactory(format, filesPerTable),
      new HDFSFileSystemContextFactory(format, filesPerTable)
    )
  }.flatten

  val sqlFileSystemContextFactories: List[TestContextFactory] = {
    for {
      format <- fileFormatOptions
      filesPerTable <- filesPerTableOptions
      idGeneration <- idGenerationOptions
    } yield SQLWithLocalFSContextFactory(format, filesPerTable, idGeneration)
  }

  // TODO: Test session PGDS

  val sqlHiveContextFactories: List[TestContextFactory] = idGenerationOptions.map(SQLWithHiveContextFactory)

  val sqlH2ContextFactories: List[TestContextFactory] = idGenerationOptions.map(SQLWithH2ContextFactory)

  val allSqlContextFactories: List[TestContextFactory] = {
    sqlFileSystemContextFactories ++ sqlHiveContextFactories ++ sqlH2ContextFactories
  }

  executeScenariosWithContext(cypher10Scenarios, Neo4jContextFactory)

  executeScenariosWithContext(allScenarios, SessionContextFactory)

  allSqlContextFactories.foreach(executeScenariosWithContext(allScenarios, _))

  fileSystemContextFactories.foreach(executeScenariosWithContext(allScenarios, _))

  case object SessionContextFactory extends CAPSTestContextFactory {

    override def toString: String = s"SESSION-PGDS"

    override def initPgds(graphNames: List[GraphName]): PropertyGraphDataSource = {
      val pgds = new SessionGraphDataSource
      graphNames.foreach(gn => pgds.store(gn, graph(gn)))
      pgds
    }
  }

  case class SQLWithH2ContextFactory(
    idGenerationStrategy: IdGenerationStrategy
  ) extends SQLContextFactory {

    override def toString: String = s"SQL-PGDS-H2-${idGenerationStrategy.toString}"

    override def initializeContext(graphNames: List[GraphName]): TestContext = {
      createH2Database(sqlDataSourceConfig, databaseName)
      super.initializeContext(graphNames)
    }

    override def releaseContext(implicit ctx: TestContext): Unit = {
      super.releaseContext
      dropH2Database(sqlDataSourceConfig, databaseName)
    }

    override def writeTable(df: DataFrame, tableName: String): Unit = {
      df.saveAsSqlTable(sqlDataSourceConfig, tableName)
    }

    override def sqlDataSourceConfig: SqlDataSourceConfig.Jdbc = {
      SqlDataSourceConfig.Jdbc(
        url = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
        driver = "org.h2.Driver",
        options = Map(
          "user" -> "sa",
          "password" -> "1234"
        )
      )
    }

  }

  case class SQLWithHiveContextFactory(
    idGenerationStrategy: IdGenerationStrategy
  ) extends SQLContextFactory {

    override def toString: String = s"SQL-PGDS-HIVE-${idGenerationStrategy.toString}"

    override def initializeContext(graphNames: List[GraphName]): TestContext = {
      createHiveDatabase(databaseName)
      super.initializeContext(graphNames)
    }

    override def releaseContext(implicit ctx: TestContext): Unit = {
      super.releaseContext
      dropHiveDatabase(databaseName)
    }

    override def writeTable(df: DataFrame, tableName: String): Unit = {
      df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    }

    override def sqlDataSourceConfig: SqlDataSourceConfig.Hive.type = Hive

  }

  case class SQLWithLocalFSContextFactory(
    override val fileFormat: FileFormat,
    override val filesPerTable: Int,
    idGenerationStrategy: IdGenerationStrategy
  ) extends LocalFileSystemContextFactory(fileFormat, filesPerTable) with SQLContextFactory {

    override def toString: String = s"SQL-PGDS-${fileFormat.name.toUpperCase}-FORMAT-$filesPerTable-FILE(S)-PER-TABLE-${idGenerationStrategy.toString}"

    override def writeTable(df: DataFrame, tableName: String): Unit = {
      val path = basePath + s"/${tableName.replace(s"$databaseName.", "")}"
      df.write.mode(SaveMode.Overwrite).option("header", "true").format(fileFormat.name).save(path)
    }

    override def sqlDataSourceConfig: SqlDataSourceConfig = {
      SqlDataSourceConfig.File(fileFormat, Some(basePath.replace("\\", "/")))
    }
  }

  trait SQLContextFactory extends CAPSTestContextFactory {

    def writeTable(df: DataFrame, tableName: String): Unit

    def sqlDataSourceConfig: SqlDataSourceConfig

    def idGenerationStrategy: IdGenerationStrategy

    protected val dataSourceName = "DS"

    protected val databaseName = "SQLPGDS"

    override def releasePgds(implicit ctx: TestContext): Unit = {
      // SQL PGDS does not support graph deletion
    }

    override def initPgds(graphNames: List[GraphName]): SqlPropertyGraphDataSource = {
      val ddls = graphNames.map { gn =>
        val g = graph(gn)
        val schema = g.schema
        schema.labelCombinations.combos.foreach { labelCombination =>
          val nodeDf = g.canonicalNodeTable(labelCombination).removePrefix(propertyPrefix)
          val tableName = databaseName + "." + nodeTableDirectoryName(labelCombination)
          writeTable(nodeDf, tableName)
        }
        schema.relationshipTypes.foreach { relType =>
          val relDf = g.canonicalRelationshipTable(relType).removePrefix(propertyPrefix)
          val tableName = databaseName + "." + relType
          writeTable(relDf, tableName)
        }
        g.defaultDdl(gn, Some(dataSourceName), Some(databaseName))
      }
      val ddl = ddls.foldLeft(graphddl.GraphDdl(Map.empty[GraphName, Graph]))(_ ++ _)
      SqlPropertyGraphDataSource(ddl, Map(dataSourceName -> sqlDataSourceConfig), idGenerationStrategy)
    }

  }

  case object Neo4jContextFactory extends CAPSTestContextFactory {

    override def toString: String = s"NEO4J-PGDS"

    override def initPgds(graphNames: List[GraphName]): PropertyGraphDataSource = {
      val pgds = CypherGraphSources.neo4j(neo4jConfig)
      // Delete graphs that were detected via meta labels again. Neo4j caches meta labels, even if no nodes with them are present.
      pgds.graphNames.filter(_ != MetaLabelSupport.entireGraphName).foreach(pgds.delete)
      graphNames.foreach(gn => pgds.store(gn, graph(gn)))
      pgds
    }

    override def releasePgds(implicit ctx: TestContext): Unit = {
      pgds.graphNames.filter(_ != MetaLabelSupport.entireGraphName).foreach(pgds.delete)
    }

  }

  class HDFSFileSystemContextFactory(
    val fileFormat: FileFormat,
    val filesPerTable: Int
  ) extends FileSystemContextFactory {

    override def toString: String = s"HDFS-PGDS-${fileFormat.name.toUpperCase}-FORMAT-$filesPerTable-FILE(S)-PER-TABLE"

    // Set an incompatible default filesystem to ensure that picking the right filesystem based on the protocol works
    def cfg: Configuration = {
      val miniClusterCfg = clusterConfig
      val incompatibleDefault = s"s3a://bucket/"
      miniClusterCfg.set("fs.defaultFS", incompatibleDefault)
      miniClusterCfg
    }

    protected def resetHadoopConfig(): Unit = {
      sparkSession.sparkContext.hadoopConfiguration.clear()
      val hadoopParams = cfg.asScala
      for (entry <- hadoopParams) {
        sparkSession.sparkContext.hadoopConfiguration.set(entry.getKey, entry.getValue)
      }
    }

    override def initializeContext(graphNames: List[GraphName]): TestContext = {
      // DO NOT DELETE THIS! If the config is not cleared, the PGDSAcceptanceTest fails because of connecting to a
      // non-existent HDFS cluster. We could not figure out why the afterEach call in MiniDFSClusterFixture does not handle
      // the clearance of the config correctly.
      resetHadoopConfig()
      super.initializeContext(graphNames)
    }

    override def releaseContext(implicit ctx: TestContext): Unit = {
      super.releaseContext
      val fs = cluster.getFileSystem()
      fs.listStatus(new Path("/")).foreach { f =>
        fs.delete(f.getPath, true)
      }
    }

    override def graphSourceFactory: FSGraphSourceFactory = {
      GraphSources.fs(hdfsURI.toString, filesPerTable = Some(filesPerTable))
    }
  }

  class LocalFileSystemContextFactory(
    val fileFormat: FileFormat,
    val filesPerTable: Int
  ) extends FileSystemContextFactory {

    override def toString: String = s"LocalFS-PGDS-${fileFormat.name.toUpperCase}-FORMAT-$filesPerTable-FILE(S)-PER-TABLE"

    protected val schemePrefix = "file://"

    protected var tempDir: TemporaryFolder = _

    def basePath: String = schemePrefix + Paths.get(tempDir.getRoot.getAbsolutePath)

    def graphSourceFactory: FSGraphSourceFactory = GraphSources.fs(basePath, filesPerTable = Some(filesPerTable))

    override def initializeContext(graphNames: List[GraphName]): TestContext = {
      tempDir = new TemporaryFolder()
      tempDir.create()
      super.initializeContext(graphNames)
    }

    override def releaseContext(implicit ctx: TestContext): Unit = {
      super.releaseContext
      tempDir.delete()
    }

  }

  trait FileSystemContextFactory extends CAPSTestContextFactory {

    def fileFormat: FileFormat

    def graphSourceFactory: FSGraphSourceFactory

    override def initPgds(graphNames: List[GraphName]): PropertyGraphDataSource = {
      val pgds = fileFormat match {
        case FileFormat.csv => graphSourceFactory.csv
        case FileFormat.parquet => graphSourceFactory.parquet
        case FileFormat.orc => graphSourceFactory.orc
      }
      graphNames.foreach(gn => pgds.store(gn, graph(gn)))
      pgds
    }

  }

  override def dataFixture: String = ""
}