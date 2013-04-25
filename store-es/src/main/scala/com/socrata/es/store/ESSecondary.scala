package com.socrata.es.store

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.es.rows.Converter
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader.Delogger._
import com.socrata.datacoordinator.id.{RowId, DatasetId}
import com.socrata.datacoordinator.secondary.Secondary
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLTypeContext}
import com.socrata.datacoordinator.truth.loader.sql.SqlDelogger
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapReader
import com.socrata.datacoordinator.truth._
import com.socrata.datacoordinator.truth.loader._
import com.socrata.es.meta._
import com.rojoma.simplearm._
import java.sql.Connection
import com.typesafe.config.{ConfigFactory, Config}
import ESScheme._
import com.socrata.es.gateway.{ESHttpGateway, ESGateway}
import loader.{Delete, Insert, Update}
import metadata.{ColumnInfo, CopyInfo}
import com.socrata.es.meta.ESColumnName
import com.socrata.es.exception.GatewayNotFound
import com.socrata.datacoordinator.util.NoopTimingReport
import com.socrata.soql.types.{SoQLValue, SoQLType, SoQLAnalysisType}
import com.socrata.soql.environment.{ColumnName, TypeName}

class ESSecondarySoQL(config: Config) extends ESSecondary[SoQLAnalysisType, SoQLValue](config)

class ESSecondary[CT, CV: Converter](val config: Config) extends Secondary[CT, CV] with ESSchemaLoader {

  import ESSecondary._


  private val rowConverter = implicitly[Converter[CV]]

  private val esBaseUrl = config.getString("url")

  def shutdown() { /* Nothing for this store */ }

  /**
   * Return the copy version sored in Elasticsearch /index/meta
   * The Elasticsearch call does not care about ESType here.
   */
  def currentVersion(datasetId: DatasetId, cookie: Cookie): Long = {
    try {
      getESGateway(datasetId).getDatasetMeta.map(_.version).getOrElse(0)
    } catch {
      case e: GatewayNotFound =>
        0
    }
  }

  def currentCopyNumber(datasetId: DatasetId, cookie: Cookie): Long = {
    try {
      getESGateway(datasetId).getDatasetMeta.map(_.copyId).getOrElse(0)
    } catch {
      case e: GatewayNotFound =>
        0
    }
  }

  def wantsWorkingCopies = false

  def dropDataset(datasetId: DatasetId, cookie: Cookie) {
    getESGateway(datasetId).deleteIndex()
  }

  def dropCopy(datasetId: DatasetId, copyNumber: Long, cookie: Cookie): Cookie = {
    getESGateway(datasetId, copyNumber).deleteType()
    cookie
  }

  def snapshots(datasetId: DatasetId, cookie: Cookie): Set[Long] = {
    try {
      getESGateway(datasetId).copies
    } catch {
      case e: GatewayNotFound => Set.empty[Long]
    }
  }

  def resync(copyContext: DatasetCopyContext[CT], cookie: Cookie, rows: Managed[Iterator[Row[CV]]]): Cookie = {
    val copyInfo = copyContext.copyInfo
    val schema = copyContext.schema
    val sidColumnName = ColumnName(":id")
    val sidColumnInfo: ColumnInfo[CT] = schema.values.find(_.logicalName == sidColumnName).get
    val esGateway = getESGateway(copyInfo)
    createSchema(copyInfo.datasetInfo.systemId, esGateway, schema)

    for (iter <- rows; row <- iter) {
      val rowId = rowConverter.rowId(sidColumnInfo, row).get
      upsert(schema, esGateway, rowId, row)
    }

    esGateway.flush()
    esGateway.setDatasetMeta(copyInfo)
    cookie
  }

  def version(datasetId: DatasetId, dataVersion: Long, cookie: Cookie, events: Iterator[Delogger.LogEvent[CV]]): Cookie = {

    validateVersionConsistency(datasetId, dataVersion, cookie)
    val copyId = currentCopyNumber(datasetId, cookie)
    val esGateway = getESGateway(datasetId, copyId)
    val schema = loadColumnIdNameMap(datasetId, esGateway)

    events.foreach { (rowDataOperation: LogEvent[CV]) => rowDataOperation match {
      case r@RowDataUpdated(xxx) =>
        r.operations.foreach {
          case Insert(sid, row) =>
            upsert(schema, esGateway, sid, row)
          case Update(sid, row) =>
            upsert(schema, esGateway, sid, row)
          case Delete(sid) =>
            delete(esGateway, sid)
        }
        esGateway.flush()
      case WorkingCopyCreated(datasetInfo: UnanchoredDatasetInfo, unanchoredCopyInfo: UnanchoredCopyInfo) =>
        createCopy(datasetInfo, unanchoredCopyInfo)
        esGateway.setDatasetMeta(unanchoredCopyInfo)
      case ColumnCreated(info: UnanchoredColumnInfo) =>
        createColumn(datasetId, copyId, info)
      case SnapshotDropped(info: UnanchoredCopyInfo) =>
        esGateway.deleteType()
      case ColumnRemoved(_) | WorkingCopyCreated(_, _) =>
        throw new UnsupportedOperationException("call resync when publish is called")
      case RowIdentifierSet(_) => // nothing to do
      case RowIdentifierCleared(_) => // nothing to do
      case otherOps =>
        // TODO:
        println(s"ignore: $otherOps")
    }}
    cookie
  }

  private def validateVersionConsistency(datasetId: DatasetId, dataVersion: Long, cookie: Cookie) {
    val curVer = currentVersion(datasetId, cookie)
    if (dataVersion - curVer > 1)
      throw new Exception(s"out of sync $curVer $dataVersion, need resync.") // TODO: Exception to be defined
  }

  private def createCopy(datasetInfo: DatasetInfoLike, copyInfo: CopyInfoLike) {
    val esGateway = getESGateway(datasetInfo.systemId, copyInfo.copyNumber)
    esGateway.ensureIndex()
  }

  private def createColumn(datasetId: DatasetId, copyId: Long, columnInfo: UnanchoredColumnInfo) {
    val esGateway = getESGateway(datasetId, copyId)
    val esColumnsMap = Map(toESColumn(columnInfo))
    esGateway.updateEsColumnMapping(esColumnsMap)
    createColumnIdMap(datasetId, Seq(columnInfo))
  }

  private def upsert(schema: ColumnIdMap[ColumnInfoLike], esGateway: ESGateway, sid: RowId, data: Row[CV]) {
    val row = rowConverter.toRow(schema, data)
    esGateway.addRow(row, sid.underlying)
  }

  private def delete(esGateway: ESGateway, sid: RowId) {
    esGateway.deleteRow(sid.underlying)
  }

  private def createSchema(datasetId: DatasetId, esGateway: ESGateway, schema: ColumnIdMap[ColumnInfo[CT]]) {

    val esColumnsMap = schema.foldLeft(Map.empty[ESColumnName, ESColumnMap]) { (map, colIdColInfo) =>
      colIdColInfo match {
        case (colId, colInfo) =>
          map + (toESColumnName(colInfo) -> ESColumnMap(SoQLType.typesByName(TypeName(colInfo.typeName))))
      }
    }
    esGateway.ensureIndex()
    deleteColumnIdMap(datasetId)
    esGateway.deleteType()
    esGateway.updateEsColumnMapping(esColumnsMap)
    createColumnIdMap(datasetId, schema.values.toSeq)
  }

  private def getESGateway(copyInfo: CopyInfo): ESGateway = {
    getESGateway(copyInfo.datasetInfo.systemId, copyInfo.copyNumber)
  }

  private def getESGateway(datasetId: DatasetId, copyId: Long = 0): ESGateway =
    new ESHttpGateway(datasetId,  copyId, esBaseUrl = esBaseUrl)
}

object ESSecondary {

  implicit def copyInfoToDatasetMeta(copyInfo: CopyInfo): DatasetMeta = DatasetMeta(copyInfo.copyNumber, copyInfo.dataVersion)

  implicit def unanchoredCopyInfoToDatasetMeta(copyInfo: UnanchoredCopyInfo): DatasetMeta = DatasetMeta(copyInfo.copyNumber, copyInfo.dataVersion)

  private def toESColumn(colInfo: UnanchoredColumnInfo) = {
    (toESColumnName(colInfo) -> ESColumnMap(SoQLType.typesByName(TypeName(colInfo.typeName))))
  }

  def shipToSecondary(rawDatasetId: Long, conn: Connection) {
    val datasetMapReader = new PostgresDatasetMapReader(conn, SoQLTypeContext.typeNamespace, NoopTimingReport)

    val datasetId = new DatasetId(rawDatasetId)
    datasetMapReader.datasetInfo(datasetId) match {
      case Some(datasetInfo) =>
        val copyInfo: CopyInfo = datasetMapReader.latest(datasetInfo)
        println( s"logTableName: ${datasetInfo.logTableName}")
        println( s"copyDataVersion: ${copyInfo.dataVersion}")
        println( s"copyNumber: ${copyInfo.copyNumber}")

        val secondary: ESSecondary[SoQLType, SoQLValue] = new com.socrata.es.store.ESSecondary[SoQLType, SoQLValue](ConfigFactory.empty)
        val delogger: Delogger[SoQLValue] = new SqlDelogger(conn, datasetInfo.logTableName, () => SoQLRowLogCodec)

        using(delogger.delog(copyInfo.dataVersion)) { logEventIter =>
          secondary.version(copyInfo.datasetInfo.systemId, copyInfo.copyNumber, None, logEventIter)
        }
      case None =>
        println(s"cannot find datases ${rawDatasetId}")
    }
  }
}
