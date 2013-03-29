package com.socrata.es.gateway

import java.{lang => jl}

import com.socrata.soql.types._
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.collection.OrderedMap
import com.socrata.es.meta.{ESColumnMap, ESColumnName, DatasetMeta}
import java.io.InputStream

class ESTestGateway(datasetContext: Option[DatasetContext[SoQLType]] = None) extends ESGateway {

  def ensureIndex() = throw new UnsupportedOperationException

  def allocateID: Long = throw new UnsupportedOperationException

  def addRow(data: Map[String, Any]) { addRow(data, allocateID) }

  def addRow(data: Map[String, Any], id: jl.Long) {
    throw new UnsupportedOperationException
  }

  def flush() { }

  def deleteRow(id: jl.Long) {
    throw new UnsupportedOperationException
  }

  def getRow(id: jl.Long): String = throw new UnsupportedOperationException

  def getRows(limit: Int, offset: Long): String = throw new UnsupportedOperationException

  def search(query: String): InputStream = throw new UnsupportedOperationException

  def deleteIndex() { throw new UnsupportedOperationException }

  def deleteType() { throw new UnsupportedOperationException }

  def updateEsColumnMapping(cols: Map[ESColumnName, ESColumnMap]) {
    throw new UnsupportedOperationException
  }

  def getDataContext(): DatasetContext[SoQLType] =
    datasetContext.getOrElse(ESTestGateway.datasetCtx)

  def getDatasetMeta(): Option[DatasetMeta] = { throw new UnsupportedOperationException }

  def setDatasetMeta(datasetMeta: DatasetMeta) { throw new UnsupportedOperationException }

  def copies: Set[Long]  = { throw new UnsupportedOperationException }
}

object ESTestGateway {

  implicit val datasetCtx = new DatasetContext[SoQLType] {
    private implicit def ctx = this
    val locale = com.ibm.icu.util.ULocale.ENGLISH
    val schema = OrderedMap(
      ColumnName(":id") -> SoQLNumber,
      ColumnName(":updated_at") -> SoQLFixedTimestamp,
      ColumnName(":created_at") -> SoQLFixedTimestamp,
      ColumnName("id") -> SoQLNumber,
      ColumnName("case_number") -> SoQLText,
      ColumnName("date") -> SoQLFixedTimestamp,
      ColumnName("updated_on") -> SoQLFixedTimestamp,
      ColumnName("primary_type") -> SoQLText,
      ColumnName("description") -> SoQLText,
      ColumnName("arrest") -> SoQLBoolean,
      ColumnName("x_coordinate") -> SoQLNumber,
      ColumnName("y_coordinate") -> SoQLNumber,
      ColumnName("fbi_code") -> SoQLText,
      ColumnName("year") -> SoQLNumber,
      ColumnName("location") -> SoQLLocation,
      ColumnName("ward") -> SoQLMoney,
      ColumnName("object") -> SoQLObject,
      ColumnName("array") -> SoQLArray
    )
  }
}
