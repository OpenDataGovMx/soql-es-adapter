package com.socrata.es.store

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.es.rows.Converter
import com.socrata.rows.{ESGateway, ESColumnMap, ESHttpGateway}
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.loader.Delogger._
import com.socrata.datacoordinator.id.{ColumnId, RowId, DatasetId, CopyId}
import com.socrata.datacoordinator.secondary.Secondary
import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.common.soql.{SoQLRowLogCodec, SoQLTypeContext}
import com.socrata.datacoordinator.truth.loader.sql.SqlDelogger
import java.sql.Connection
import sql.PostgresDatasetMapReader
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.loader._
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.truth.loader.Insert
import com.socrata.es.meta.{DatasetMeta, ESIndex, ESType}

class ESSecondary[CV: Converter](conn: Connection) extends Secondary[CV] {

  import ESSecondary._

  private val rowConverter = implicitly[Converter[CV]]

  /**
   * Return the copy version sored in Elasticsearch /index/meta
   * The Elasticsearch call does not care about ESType here.
   */
  def currentVersion(datasetID: DatasetId): Long = {
    (new ESHttpGateway(datasetID)).getDatasetMeta.map(_.copyId).getOrElse(0)
  }

  def wantsSnapshots = false

  def resync(copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo], rows: Iterator[com.socrata.datacoordinator.Row[CV]]) {
    val sidColumnInfo: ColumnInfo = schema.values.find(_.logicalName == ":id").get
    val esGateway: ESHttpGateway = getESGateway(copyInfo)
    createSchema(esGateway, schema)
    rows.foreach { row =>
      val rowId = rowConverter.rowId(sidColumnInfo, row).get
      upsert(schema, esGateway, rowId, row)
    }
    esGateway.flush()
    esGateway.setDatasetMeta(copyInfo)
  }

  def version(copyInfo: CopyInfo, events: Iterator[Delogger.LogEvent[CV]]) {
    val esGateway = getESGateway(copyInfo)

    // TODO: consider validating schema or getting schema from Elasticsearch
    val schema = schemaFromPg(conn, copyInfo)

    events.foreach { (rowDataOperation: LogEvent[CV]) => rowDataOperation match {
      case r@RowDataUpdated(xxx) =>
        r.operations.foreach {
          case Insert(sid, row) =>
            upsert(schema, esGateway, sid, row)
          case Update(sid, row) =>
            upsert(schema, esGateway, sid, row)
          case Delete(sid) =>
            //TODO:
            println(s"ignore delete $sid")
        }
        esGateway.flush()
      case WorkingCopyCreated(datasetInfo: UnanchoredDatasetInfo, unanchoredCopyInfo: UnanchoredCopyInfo) =>
        createCopy(copyInfo)
      case ColumnCreated(info: UnanchoredColumnInfo) =>
        createColumn(copyInfo, info)
      case RowIdentifierSet(_) => // nothing to do
      case RowIdentifierCleared(_) => // nothing to do
      case otherOps =>
        // TODO:
        println(s"ignore: $otherOps")
    }}
    esGateway.setDatasetMeta(copyInfo)
  }

  private def createCopy(copyInfo: CopyInfo) {
    val esGateway = getESGateway(copyInfo)
    esGateway.ensureIndex()
  }

  private def createColumn(copyInfo: CopyInfo, columnInfo: UnanchoredColumnInfo) {
    val esGateway = getESGateway(copyInfo)
    val esColumnsMap = Map(toESColumn(columnInfo))
    esGateway.updateEsColumnMapping(esColumnsMap)
  }

  private def upsert(schema: ColumnIdMap[ColumnInfo], esGateway: ESGateway, sid: RowId, data: Row[CV]) {
    val row = rowConverter.toRow(schema, data)
    esGateway.addRow(row, sid.underlying)
  }

  private def createSchema(esGateway: ESGateway, schema: ColumnIdMap[ColumnInfo]) {
    val esColumnsMap = schema.foldLeft(Map.empty[String, ESColumnMap]) { (map, colIdColInfo) =>
      colIdColInfo match {
        case (colId, colInfo) =>
          val colName = colInfo.physicalColumnBaseBase
          map + (colName -> ESColumnMap(SoQLTypeContext.typeFromName(colInfo.typeName)))
      }
    }
    esGateway.ensureIndex()
    esGateway.deleteType()
    esGateway.updateEsColumnMapping(esColumnsMap)
  }
}

object ESSecondary {

  implicit def datasetIdToESIndex(datasetId: DatasetId): ESIndex = ESIndex(s"ds${datasetId.underlying}")

  implicit def versionToIndexType(version: CopyId): ESType = ESType(s"v${version.underlying}")

  implicit def copyInfoToDatasetMeta(copyInfo: CopyInfo): DatasetMeta = DatasetMeta(copyInfo.systemId.underlying, copyInfo.dataVersion)

  private def getESGateway(copyInfo: CopyInfo) =
    new ESHttpGateway(copyInfo.datasetInfo.systemId, copyInfo.systemId)

  private def schemaFromPg(conn: Connection, copyInfo: CopyInfo) = {
    val datasetMapReader = new PostgresDatasetMapReader(conn)

    Option(copyInfo.datasetInfo.systemId).flatMap(datasetMapReader.datasetInfo).flatMap(datasetMapReader.copyNumber(_, copyInfo.copyNumber)) match {
      case Some(ci) => datasetMapReader.schema(ci)
      case _ => throw new Exception("schema not found")
    }
  }

  private def toESColumn(colInfo: UnanchoredColumnInfo) = {
    (colInfo.physicalColumnBaseBase -> ESColumnMap(SoQLTypeContext.typeFromName(colInfo.typeName)))
  }

  def shipToSecondary(datasetName: String, conn: Connection) {
    val datasetMapReader = new PostgresDatasetMapReader(conn)

    datasetMapReader.datasetInfo(datasetName) match {
      case Some(datasetInfo) =>
        val copyInfo: CopyInfo = datasetMapReader.latest(datasetInfo)
        println( s"logTableName: ${datasetInfo.logTableName}")
        println( s"copyDataVersion: ${copyInfo.dataVersion}")
        println( s"copyNumber: ${copyInfo.copyNumber}")

        val secondary: ESSecondary[Any] = new com.socrata.es.store.ESSecondary[Any](conn)
        val delogger: Delogger[Any] = new SqlDelogger(conn, datasetInfo.logTableName, () => SoQLRowLogCodec)

        using(delogger.delog(copyInfo.dataVersion)) { logEventIter =>
          secondary.version(copyInfo, logEventIter)
        }
      case None =>
        println(s"cannot find datases $datasetName")
    }
  }

  def resyncSecondary(datasetName: String, schema: ColumnIdMap[ColumnInfo], conn: Connection, rows: Iterator[ColumnIdMap[_]]) {

    val datasetMapReader = new PostgresDatasetMapReader(conn)

    datasetMapReader.datasetInfo(datasetName) match {
      case Some(datasetInfo) =>
        val copyInfo: CopyInfo = datasetMapReader.latest(datasetInfo)
        val secondary: ESSecondary[Any] = new com.socrata.es.store.ESSecondary[Any](conn)
        secondary.resync(copyInfo, schema, rows)
      case None =>
        println(s"cannot find datases $datasetName")
    }
  }
}