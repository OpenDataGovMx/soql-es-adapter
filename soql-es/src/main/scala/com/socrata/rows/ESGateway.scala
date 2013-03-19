package com.socrata.rows

import java.{lang => jl}
import com.ning.http.client.{Response, AsyncHttpClient, AsyncHttpClientConfig}
import com.rojoma.json.ast._
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.{mutable => scm }
import com.socrata.soql.types.SoQLType
import com.rojoma.json.util.JsonUtil
import com.socrata.json.codec.elasticsearch.DatasetContextCodec
import java.io.InputStream
import com.socrata.soql.environment.DatasetContext
import com.socrata.es.meta.{ESColumnName, DatasetMeta, ESType, ESIndex}
import com.socrata.es.exception._
import com.rojoma.json.io.JsonReader


trait ESGateway {
  def ensureIndex()

  def addRow(data: Map[String, Any])

  def addRow(data: Map[String, Any], id: jl.Long)

  def deleteRow(id: jl.Long)

  def getRow(id: jl.Long): String

  def getRows(limit: Int, offset: Long): String

  def updateEsColumnMapping(cols: Map[ESColumnName, ESColumnMap])

  def getDataContext(): DatasetContext[SoQLType]

  def deleteIndex()

  def deleteType()

  def flush(): Unit

  def getDatasetMeta(): Option[DatasetMeta]

  def setDatasetMeta(datasetMeta: DatasetMeta)

  def search(query: String): InputStream

  def copies: Set[Long]
}

class ESHttpGateway(val esIndex: ESIndex, val esType: ESType = ESType("data"),
                    val batchSize: Int = 500,
                    val esBaseUrl: String = "http://localhost:9200"
                     ) extends ESGateway {

  import ESHttpGateway._

  private val esDsUrl = s"$esBaseUrl/$esIndex/$esType"

  private var totalRowsAdded: Long = 0

  private var lastTime: Long  = 0

  private val buffer: scm.ListBuffer[AnyRef] = new scm.ListBuffer[AnyRef]()

  private var bufSize = 0

  def allocateID: Long =
    jl.Long.parseLong(
      IdDateFormat.format(new Date(System.currentTimeMillis())) +
        "%04d".format(bufSize)
    )


  def addRow(data: Map[String, Any]) { addRow(data, allocateID) }

  def addRow(data: Map[String, Any], id: jl.Long) {

    totalRowsAdded += 1
    bufSize += 1
    buffer += JObject(Map("create" -> JObject(Map("_id" -> JNumber(id.longValue()))))).toString
    buffer += data.map { case (k, v) => JString(k) + " : " + v.toString }
      .mkString("{" , ",", "}")

    if (totalRowsAdded % 1000 == 0) {
      val timeSpent = (System.currentTimeMillis() - lastTime) / 1000
      println("rows added : %d - %d s %d".format(totalRowsAdded, timeSpent, id))
      lastTime = System.currentTimeMillis()
    }
    if (totalRowsAdded % batchSize == 0) {
      flush()
    }
  }

  def flush() {
    if (!buffer.isEmpty) {
      val url = "%1$s/_bulk".format(esDsUrl)
      val body = buffer.mkString("", "\n", "\n")
      execute(Client.preparePost(url).setBody(body ))
      buffer.clear()
      bufSize = 0
    }
  }

  def deleteRow(id: jl.Long) {
    val url = "%1$s/%2$d".format(esDsUrl, id)
    execute(Client.prepareDelete(url))
  }

  def getRow(id: jl.Long): String = {
    val url = "%1$s/_search?q=_id:%2$d".format(esDsUrl, id)
    execute(Client.prepareGet(url))
  }

  def getRows(limit: Int, offset: Long): String = {
    val url = "%1$s/_search?size=%2$d&from=%3$d".format(esDsUrl, limit, offset)
    execute(Client.prepareGet(url))
  }

  def search(query: String): InputStream = {
    val url = "%1$s/_search?pretty=true".format(esDsUrl)
    getResponse(Client.preparePost(url).setBody(query)).getResponseBodyAsStream
  }

  def updateEsColumnMapping(cols: Map[ESColumnName, ESColumnMap]) {
    val propMap = cols.map { case (k, v) => k.toString -> v.propMap }
    val jProperties = JObject(Map("properties" -> JObject(propMap)))
    val jBody = JObject(Map(esIndex.raw -> jProperties))
    execute(Client.preparePost("%1$s/_mapping".format(esDsUrl)).setBody(jBody.toString))
  }

  def getDataContext(): DatasetContext[SoQLType] = {
    val url = "%1$s/_mapping".format(esDsUrl)
    val result = execute(Client.prepareGet(url))
    implicit val dsCtxCodec = new DatasetContextCodec(esType = esType)
    val dsCtx: DatasetContext[SoQLType] = JsonUtil.parseJson(result).get
    dsCtx
  }

  def getDatasetMeta(): Option[DatasetMeta] = {
    val result = execute(Client.prepareGet(metaUrl))
    import DatasetMeta.datasetMetaJCodec
    JsonUtil.parseJson(result)
  }

  def setDatasetMeta(datasetMeta: DatasetMeta) {
    import DatasetMeta.datasetMetaJCodec
    val body = JsonUtil.renderJson(datasetMeta)
    execute(Client.preparePost(metaUrl).setBody(body))
  }

  def copies: Set[Long] = {
    val url = s"$indexUrl/_mapping"
    val s = execute(Client.prepareGet(url))

    JsonReader.fromString(s) match {
      case JObject(fields) =>
        val muSet = fields(esIndex.raw) match {
          case JObject(fields) =>
            val CopyNumberRx = "v([0-9]+)".r
            fields.keySet.collect {
              case CopyNumberRx(copyNumber) => copyNumber.toLong
            }
          case _ =>
            Set.empty[Long]
        }
        muSet.toSet // make immutable
      case _ =>
        Set.empty[Long]
    }
  }

  private def metaUrl = s"$esBaseUrl/$esIndex/meta/1"

  private def indexUrl = s"$esBaseUrl/$esIndex"

  /**
   * Create index if it does not exist
   */
  def ensureIndex() {
    val response = Client.prepareHead(indexUrl).execute().get()
    response.getStatusCode match {
      case 404 =>
        execute(Client.preparePost(indexUrl))
      case sc if (sc >= 200 && sc <= 299) =>
      case _ =>
        throw new Exception(s"Unknown problem with the index $esIndex")
    }
  }

  def deleteIndex() {
    val response = Client.prepareDelete(indexUrl).execute().get()
    response.getStatusCode match {
      case 404 =>
        println("delete non-existing index " + indexUrl)
      case sc if (sc >= 200 && sc <= 299) =>
        println("delete existing index " + indexUrl)
      case sc =>
        throw new Exception(s"error in deleting index $esDsUrl status code: $sc")
    }
  }

  def deleteType() {
    val response = Client.prepareDelete(esDsUrl).execute().get()
    response.getStatusCode match {
      case 404 =>
        println("delete non-existing index type" + esDsUrl)
      case sc if (sc >= 200 && sc <= 299) =>
        println("delete existing index type" + esDsUrl)
      case sc =>
        throw new Exception(s"error in deleting index $esDsUrl status code: $sc")
    }
  }
}

object ESHttpGateway {

  private val ClientConfig = new AsyncHttpClientConfig.Builder()
    .setRequestTimeoutInMs(60000 * 60) // request timeout in 1 hour
    .setConnectionTimeoutInMs(60000 * 3).build() // connection timeout in 3 minutes

  println("timeout config: request - %d, connection -%d".format(
    ClientConfig.getRequestTimeoutInMs, ClientConfig.getConnectionTimeoutInMs))

  private val Client = new AsyncHttpClient(ClientConfig)

  private val IdDateFormat = new SimpleDateFormat("yyMMddHHmmssSSS")

  def getResponse(requestBuilder: AsyncHttpClient#BoundRequestBuilder): Response = {
    val rf = requestBuilder.execute()
    try {
      val response: Response = rf.get()
      if (response.getStatusCode < 200 || response.getStatusCode > 299) {
        throw GatewayException(response.getStatusCode, response.getStatusText)
      }
      response
    } catch {
      case ex: java.util.concurrent.ExecutionException =>
        throw ex.getCause
    }
  }

  def execute(requestBuilder: AsyncHttpClient#BoundRequestBuilder): String = {
    getResponse(requestBuilder).getResponseBody()
  }
}
