package com.socrata.rows

import java.{lang => jl}

import com.socrata.soql.DatasetContext
import com.socrata.soql.types._
import org.apache.commons.lang.NotImplementedException
import com.socrata.soql.names.ColumnName

class ESTestGateway(datasetContext: Option[DatasetContext[SoQLType]] = None) extends ESGateway {

  def allocateID: Long = throw new NotImplementedException

  def addRow(data: Map[String, AnyRef]) { addRow(data, allocateID) }

  def addRow(data: Map[String, AnyRef], id: jl.Long) {
    throw new NotImplementedException
  }

  def flush() { }

  def deleteRow(id: jl.Long) {
    throw new NotImplementedException
  }

  def getRow(id: jl.Long): String = throw new NotImplementedException

  def getRows(limit: Int, offset: Long): String = throw new NotImplementedException

  def search(query: String): String = throw new NotImplementedException

  def updateEsColumnMapping(cols: Map[String, ESColumnMap]) {
    throw new NotImplementedException
  }

  def getDataContext(): DatasetContext[SoQLType] =
    datasetContext.getOrElse(ESTestGateway.datasetCtx)
}

object ESTestGateway {

  implicit val datasetCtx = new DatasetContext[SoQLType] {
    private implicit def ctx = this
    val locale = com.ibm.icu.util.ULocale.ENGLISH
    val schema = com.socrata.collection.OrderedMap(
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
      ColumnName("ward") -> SoQLMoney
    )
  }
}