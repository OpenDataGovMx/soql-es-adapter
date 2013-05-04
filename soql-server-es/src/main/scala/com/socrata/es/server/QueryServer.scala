package com.socrata.es.server

import com.socrata.http.routing._
import com.socrata.http.routing.HttpMethods._
import com.socrata.http.server._
import config.QueryServerConfig
import javax.servlet.http._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.rojoma.json.util.JsonUtil
import com.socrata.es.gateway.{ESGateway, ESHttpGateway}
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.soql.types.SoQLType
import io.Codec
import com.socrata.soql.environment.ColumnName
import java.io.{InputStream, Writer, ByteArrayInputStream}
import com.rojoma.simplearm.util._
import com.socrata.soql.SoQLAnalysis
import com.socrata.es.soql.parse.ESResultSet
import com.socrata.es.soql.query.ESQuery
import com.rojoma.json.ast.JObject
import com.rojoma.json.io.PrettyJsonWriter
import org.apache.log4j.PropertyConfigurator
import com.typesafe.config.ConfigFactory
import com.socrata.es.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.es.coordinator.Schema
import com.socrata.thirdparty.typesafeconfig.Propertizer

object QueryServer {

  val config = try {
    new QueryServerConfig(ConfigFactory.load().getConfig("com.socrata.soql-server-es"))
  } catch {
    case e: Exception =>
      Console.err.println(e)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  private val log = org.slf4j.LoggerFactory.getLogger(QueryServer.getClass)

  def main(args:Array[String]) {
    val server = new SocrataServerJetty(route, port = config.port)
    log.info("starting es query server")
    server.run
  }

  private val routerSet = RouterSet(
    ExtractingRouter[HttpService](GET, "/schema")(schema _),
    ExtractingRouter[HttpService](GET, "/query")(query _),
    ExtractingRouter[HttpService](POST, "/query")(query _)
  )

  def schema()(req: HttpServletRequest): HttpResponse = {

    val datasetId = new DatasetId(req.getParameter("ds").toLong)
    val es = getESGateway(datasetId)
    val ctx = es.getDataContext
    // TODO: Change hash to sha1
    val schema = Schema(ctx.schema.hashCode.toString, ctx.schema, ColumnName(":id"))
    OK ~> ContentType("application/json; charset=utf-8") ~> Write(JsonUtil.writeJson(_, schema, buffer = true))
  }

  def query()(req: HttpServletRequest): HttpServletResponse => Unit =  {

    val datasetId = new DatasetId(req.getParameter("ds").toLong)
    val analysisParam = req.getParameter("q")
    val analysisStream = new ByteArrayInputStream(analysisParam.getBytes(Codec.UTF8.charSet))
    val schemaHash = req.getParameter("s")
    val analysis: SoQLAnalysis[SoQLType] = SoQLAnalyzerHelper.deserializer(analysisStream)
    val esGateway = getESGateway(datasetId)
    val esQuery = new ESQuery("not-used", esGateway, Some(1000))
    val qry = esQuery.full(analysis)
    val result: Stream[JObject] = using(esGateway.search(qry)) { inputStream: InputStream =>
      val (total, rowStream) = ESResultSet.parser(analysis, inputStream).rowStream()
      rowStream
    }
    OK ~> ContentType("application/json; charset=utf-8") ~> Write(writeResult(result))
  }

  private def writeResult(result: Stream[JObject]) = (writer: Writer) => {
    writer.write("[")
    result match {
      case h #:: t =>
        PrettyJsonWriter.toWriter(writer, h)
        t.foreach { row => writer.write(","); PrettyJsonWriter.toWriter(writer, row) }
      case s =>
        s.foreach { row => PrettyJsonWriter.toWriter(writer, row) }
    }
    writer.write("]")
  }

  private def route(req: HttpServletRequest): HttpResponse = {

    val parts = req.getRequestURI.split('/').tail

    routerSet(req.getMethod, parts) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound
    }
  }

  private def getESGateway(datasetId: DatasetId): ESGateway = {
    import com.socrata.es.store.ESScheme._
    val esMeta = new ESHttpGateway(datasetId, esBaseUrl = config.secondary.url)
    (for(datasetMeta <- esMeta.getDatasetMeta) yield {
      new ESHttpGateway(datasetId, datasetMeta.copyId)
    }).get
  }
}
