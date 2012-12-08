package com.socrata.rows

import java.{lang => jl}
import com.ning.http.client.{Response, AsyncHttpClient, AsyncHttpClientConfig}
import com.rojoma.json.ast._
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.{mutable => scm }
import com.socrata.exceptions.unobtainium.InternalException
import com.socrata.soql.DatasetContext
import com.socrata.soql.types.SoQLType
import com.rojoma.json.util.JsonUtil
import com.socrata.json.codec.elasticsearch.DatasetContextCodec
import java.io.InputStream

trait ESGateway {
  def addRow(data: Map[String, AnyRef])

  def addRow(data: Map[String, AnyRef], id: jl.Long)

  def deleteRow(id: jl.Long)

  def getRow(id: jl.Long): String

  def getRows(limit: Int, offset: Long): String

  def updateEsColumnMapping(cols: Map[String, ESColumnMap])

  def getDataContext(): DatasetContext[SoQLType]

  def flush(): Unit
}

class ESHttpGateway(val esIndex: String, val esType: String = "data",
                    val batchSize: Int = 500,
                    val esBaseUrl: String = "http://localhost:9200"
                     ) extends ESGateway {

  import ESHttpGateway._

  private val esDsUrl = "%1$s/%2$s/%3$s".format(esBaseUrl, esIndex, esType)

  private var totalRowsAdded: Long = 0

  private var lastTime: Long  = 0

  private val buffer: scm.ListBuffer[AnyRef] = new scm.ListBuffer[AnyRef]()

  private var bufSize = 0

  def allocateID: Long =
    jl.Long.parseLong(
      IdDateFormat.format(new Date(System.currentTimeMillis())) +
        "%04d".format(bufSize)
    )


  def addRow(data: Map[String, AnyRef]) { addRow(data, allocateID) }

  def addRow(data: Map[String, AnyRef], id: jl.Long) {

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

  def updateEsColumnMapping(cols: Map[String, ESColumnMap]) {
    val propMap = cols.map { case (k, v) => k -> v.propMap }
    val jProperties = JObject(Map("properties" -> JObject(propMap)))
    val jBody = JObject(Map(esIndex -> jProperties))
    execute(Client.preparePost("%1$s/_mapping".format(esDsUrl)).setBody(jBody.toString))
  }

  def getDataContext(): DatasetContext[SoQLType] = {
    val url = "%1$s/_mapping".format(esDsUrl)
    val result = execute(Client.prepareGet(url))
    implicit val dsCtxCodec = new DatasetContextCodec()
    val dsCtx: DatasetContext[SoQLType] = JsonUtil.parseJson(result).get
    dsCtx
  }

  /**
   * Create index if it does not exist
   */
  def ensureIndex() {
    val url = "%s/%s".format(esBaseUrl, esIndex)
    val response = Client.prepareHead(url).execute().get()
    response.getStatusCode match {
      case 404 =>
        execute(Client.preparePost(url))
      case sc if (sc >= 200 && sc <= 299) =>
      case _ =>
        throw new Exception("Unknown problem with the index %s".format(esIndex))
    }
  }
}

object ESHttpGateway {

  private val ClientConfig = new AsyncHttpClientConfig.Builder()
    .setConnectionTimeoutInMs(60000 * 3).build() // timeout in 3 minutes

  private val Client = new AsyncHttpClient(ClientConfig)

  private val IdDateFormat = new SimpleDateFormat("yyMMddHHmmssSSS")

  def getResponse(requestBuilder: AsyncHttpClient#BoundRequestBuilder): Response = {
    val rf = requestBuilder.execute()
    try {
      val response: Response = rf.get()
      if (response.getStatusCode < 200 || response.getStatusCode > 299) {
        throw new InternalException(response.getStatusText)
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
