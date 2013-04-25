package com.socrata.elasticsearch

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import org.postgresql.ds.PGSimpleDataSource
import java.sql.{DriverManager, Connection}
import com.socrata.datacoordinator.truth.sql.DatabasePopulator
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.sql.DatasetMapLimits
import com.socrata.datacoordinator.common.SoQLCommon
import com.socrata.datacoordinator.truth.universe.sql.PostgresCopyIn
import com.socrata.datacoordinator.util.NoopTimingReport
import com.socrata.datacoordinator.service.Mutator
import com.rojoma.json.ast.JValue
import com.rojoma.json.util.JsonArrayIterator
import com.rojoma.json.io.JsonEventIterator
import java.io.{InputStream, File, FileInputStream, InputStreamReader}
import com.socrata.datacoordinator.secondary.{Secondary, NamedSecondary}
import com.socrata.es.store.ESSecondarySoQL
import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, DatasetInfo}
import io.Codec
import com.socrata.es.gateway.ESHttpGateway
import com.socrata.es.meta.{ESType, ESIndex}
import com.socrata.es.soql.query.ESQuery
import com.socrata.es.soql.parse.ESResultSet
import com.socrata.datacoordinator.id.DatasetId

class SoQLTest extends FunSuite with MustMatchers with BeforeAndAfterAll {

  import SoQLTest._

  override def beforeAll() {
    println("setting up database")
    createDatabase()
    println("import a dataset")
    importDataset()
  }

  test("select * limit 2") {
    val soql = "select * limit 2"
    val (qry, analysis) = esQuery.full(soql)
    println("\nElastic Search Query String:\n" + qry)
    using(esGateway.search(qry)) { inputStream: InputStream =>
      val (total, rowStream) = ESResultSet.parser(analysis, inputStream).rowStream()
      println("\nResult total rows: %d, returned rows: %d".format(total.getOrElse(-1), rowStream.size))
      println(rowStream.mkString("[", ",\n", "]"))
    }
  }
}

object SoQLTest {

  val limit: Option[BigInt] = None
  val esGateway = new ESHttpGateway(ESIndex("ds1"), ESType("v1"))
  val esQuery = new ESQuery("", esGateway, limit)

  def importDataset() {
    withDb() { conn =>

      val ds = new PGSimpleDataSource
      ds.setServerName("localhost")
      ds.setPortNumber(5432)
      ds.setUser("blist")
      ds.setPassword("blist")
      ds.setDatabaseName("blist_test2")

      val executor = java.util.concurrent.Executors.newCachedThreadPool()

      val common = new SoQLCommon(
        ds,
        PostgresCopyIn,
        executor,
        Function.const(None),
        NoopTimingReport
      )

      val (datasetId, _) = processMutationCreate(common, fixtureFile("mutate-create.json"))
      processMutation(common, fixtureFile("mutate-publish.json"), datasetId)
      pushToSecondary(common, datasetId)
    }
  }

  def populateDatabase(conn: Connection) {
    val sql = DatabasePopulator.metadataTablesCreate(DatasetMapLimits())
    using(conn.createStatement()) { stmt =>
      stmt.execute(sql)
    }
  }

  def withDb[T]()(f: (Connection) => T): T = {
    using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/blist_test2", "blist", "blist")) { conn =>
      conn.setAutoCommit(true)
      populateDatabase(conn)
      f(conn)
    }
  }

  def createDatabase() {
    using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "blist", "blist")) { conn =>
      conn.setAutoCommit(true)
      val sql = "drop database if exists blist_test2; create database blist_test2;"
      using(conn.createStatement()) { stmt =>
        stmt.execute(sql)
      }
    }
  }

  def processMutationCreate(common: SoQLCommon, script: File) = {
    val mutator = new Mutator(common.Mutator)
    val inputStream = new FileInputStream(script)
    val jsonEventIter = new JsonEventIterator(new InputStreamReader(inputStream, Codec.UTF8.charSet))
    val jsonArrayIter = JsonArrayIterator[JValue](jsonEventIter)
    // process mutation
    for(u <- common.universe) yield {
      mutator.createScript(u, jsonArrayIter)
    }
  }

  def processMutation(common: SoQLCommon, script: File, datasetId: DatasetId) = {
    val mutator = new Mutator(common.Mutator)
    val inputStream = new FileInputStream(script)

    val jsonEventIter = new JsonEventIterator(new InputStreamReader(inputStream, Codec.UTF8.charSet))
    val jsonArrayIter = JsonArrayIterator[JValue](jsonEventIter)
    // process mutation
    for(u <- common.universe) yield {
      mutator.updateScript(u, datasetId, jsonArrayIter)
    }
  }


  def pushToSecondary(common: SoQLCommon, datasetId: DatasetId) {
    val config = ConfigFactory.load().getConfig("com.socrata.secondary-es")
    val storeId = "es"

    for(u <- common.universe) {
      val secondary = new ESSecondarySoQL(config).asInstanceOf[Secondary[u.CT, u.CV]]
      val pb = u.playbackToSecondary
      val mapReader = u.datasetMapReader
      for {
        datasetInfo <- mapReader.datasetInfo(datasetId)
      } yield {
        val copyInfo: CopyInfo = mapReader.latest(datasetInfo)
        val delogger = u.delogger(datasetInfo)
        val di: DatasetInfo = datasetInfo
        println(s"ds${di.systemId.underlying} v${copyInfo.copyNumber}")
        pb(datasetId, NamedSecondary(storeId, secondary), mapReader, delogger)
      }
    }
  }

  def fixtureFile(name: String): File = {
    val rootDir = System.getProperty("user.dir")
    new File(rootDir + "/import-es/src/test/resources/fixtures/" + name)
  }
}
