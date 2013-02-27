package com.socrata.es.rows

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.rows.ESColumnMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.Row
import com.rojoma.json.ast.JNumber
import com.socrata.datacoordinator.id.RowId

trait Converter[CV] {
  def toRow(schema: ColumnIdMap[ColumnInfo], row: Row[CV]): Map[String, Any]

  def rowId(columnInfo: ColumnInfo, row: Row[CV]): Option[RowId]
}


object Converter {

  implicit val converterFromAny = new Converter[Any] {
    def toRow(schema: ColumnIdMap[ColumnInfo], row: Row[Any]): Map[String, Any] = {

      val esColumnMap: ColumnIdMap[ESColumnMap] = schema.mapValuesStrict { (ci: ColumnInfo) => ESColumnMap(SoQLTypeContext.typeFromName(ci.typeName)) }
      row.foldLeft(Map.empty[String, Any]) { (acc, x) =>
        x match {
          case (colId, cell) =>
            acc + (schema(colId).physicalColumnBaseBase -> esColumnMap(colId).toES(cell))
        }
      }
    }

    def rowId(columnInfo: ColumnInfo, row: Row[Any]): Option[RowId] = {
      val colId = columnInfo.systemId
      val cell = row(colId)
      val esColumnMap = ESColumnMap(SoQLTypeContext.typeFromName(columnInfo.typeName))
      esColumnMap.toES(cell) match {
        case jv: JNumber => Some(new RowId(jv.toLong))
        case x => None
      }
    }
  }
}
