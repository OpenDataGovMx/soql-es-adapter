package com.socrata.es.store

import com.socrata.rows.{ESColumnMap, ESHttpGateway, ESGateway}
import com.socrata.soql.types.{SoQLText, SoQLType, SoQLNumber}
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.adapter.elasticsearch.{ESResultSet, ESQuery}
import com.rojoma.simplearm.util._
import java.io.InputStream
import com.rojoma.json.ast.{JNumber, JString, JObject}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.codec.JsonCodec._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.{UnanchoredColumnInfo, ColumnInfoLike}
import com.socrata.datacoordinator.id.{CopyId, DatasetId, RowId, ColumnId}
import com.socrata.es.meta.{ESColumnName, ESType, ESIndex}
import com.socrata.soql.collection.OrderedMap
import com.typesafe.config.Config


/**
 * column id, name map is stored in the dataset index as follows:
 * /ds/column/column_id
 * name : String
 * id : Long
 * type : SoQLType
 */
trait ESSchemaLoader {

  import ESScheme._

  val config: Config

  def createColumnIdMap(datasetId: DatasetId, columnInfos: Seq[ColumnInfoLike]) {
    val esColGateway = getColumnGateway(datasetId)
    columnInfos.foreach { ci =>
      val columnInfo = Map(
        "name" -> JString(toESColumnName(ci).toString),
        "id" -> JNumber(ci.systemId.underlying),
        "type" -> JString(ci.typeName))
      esColGateway.addRow(columnInfo, ci.systemId.underlying)
    }
    esColGateway.flush()
  }

  def loadColumnIdNameMap(datasetId: DatasetId, esGateway: ESGateway): ColumnIdMap[ColumnInfoLike] = {

    val schemaGw = getColumnGateway(datasetId)
    val dataContext: DatasetContext[SoQLType] = schemaDataContext(esGateway)
    val esIndex = datasetIdToESIndex(datasetId)
    val query = new ESQuery(esIndex.toString, schemaGw, None, Some(dataContext))
    val soql = "select * limit 1000"
    val (qry, analysis) = query.full(soql)
    val schema = using(schemaGw.search(qry)) { inputStream: InputStream =>
      val (total, rowStream) = ESResultSet.parser(analysis, inputStream).rowStream()
      rowStream.foldLeft(scala.Predef.Map.empty[ColumnId, UnanchoredColumnInfo]){ (map, jobj: JObject) =>
        val name = JsonCodec.fromJValue[String](jobj("name")).get
        val typ = JsonCodec.fromJValue[String](jobj("type")).get
        val id = new ColumnId(JsonCodec.fromJValue[Long](jobj("id")).get)
        val columnInfo = UnanchoredColumnInfo(id, name, typ, name, false, false)
        map + (id -> columnInfo)
      }
    }
    ColumnIdMap(schema)
  }

  private def schemaDataContext(esGateway: ESGateway): DatasetContext[SoQLType] = {
    new DatasetContext[SoQLType] {
      implicit val ctx = this
      val columnSchema = OrderedMap(
        ColumnName("name") -> SoQLText,
        ColumnName("type") -> SoQLText,
        ColumnName("id") -> SoQLNumber)
      val locale = com.ibm.icu.util.ULocale.ENGLISH
      val schema = columnSchema
    }
  }

  private def getColumnGateway(datasetId: DatasetId): ESGateway =
    new ESHttpGateway(datasetId,  ESType("column"), esBaseUrl = config.getString("url"))
}


object ESScheme {

  implicit def datasetIdToESIndex(datasetId: DatasetId): ESIndex = ESIndex(s"ds${datasetId.underlying}")

  implicit def copyIdToIndexType(version: CopyId): ESType = versionToIndexType(version.underlying)

  implicit def versionToIndexType(version: Long): ESType = ESType(s"v${version}")

  def toESColumnName(columnName: ESColumnName): ESColumnName = columnName

  implicit def columnInfoLikeToESColumn(ci: ColumnInfoLike): ESColumnName = {
    val pcbb = ci.physicalColumnBaseBase
    val name =
      if (pcbb.startsWith("s_")) pcbb.replace("s_", ":")
      else pcbb.replace("u_", "")
    ESColumnName(name)
  }
}
