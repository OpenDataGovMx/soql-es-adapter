package com.socrata.es.rows

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfoLike
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.Row
import com.rojoma.json.ast.JNumber
import com.socrata.datacoordinator.id.RowId
import com.socrata.es.store.ESScheme._
import com.socrata.es.meta.ESColumnMap
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.environment.TypeName

trait Converter[CV] {
  def toRow(schema: ColumnIdMap[ColumnInfoLike], row: Row[CV]): Map[String, Any]

  def rowId(columnInfo: ColumnInfoLike, row: Row[CV]): Option[RowId]
}


object Converter {

  implicit val converterFromAny = new Converter[Any] {
    def toRow(schema: ColumnIdMap[ColumnInfoLike], row: Row[Any]): Map[String, Any] = {

      val esColumnMap: ColumnIdMap[ESColumnMap] = schema.mapValuesStrict { (ci: ColumnInfoLike) => ESColumnMap(SoQLType.typesByName(TypeName(ci.typeName))) }
      row.foldLeft(Map.empty[String, Any]) { (acc, x) =>
        x match {
          case (colId, cell) =>
            acc + (toESColumnName(schema(colId)).toString -> esColumnMap(colId).toES(cell))
        }
      }
    }

    def rowId(columnInfo: ColumnInfoLike, row: Row[Any]): Option[RowId] = {
      val colId = columnInfo.systemId
      val cell = row(colId)
      val esColumnMap = ESColumnMap(SoQLType.typesByName(TypeName(columnInfo.typeName)))
      esColumnMap.toES(cell) match {
        case jv: JNumber => Some(new RowId(jv.toLong))
        case x => None
      }
    }
  }

  implicit val converterFromSoQLValue = new Converter[SoQLValue] {
    def toRow(schema: ColumnIdMap[ColumnInfoLike], row: Row[SoQLValue]): Map[String, Any] = {

      val esColumnMap: ColumnIdMap[ESColumnMap] = schema.mapValuesStrict { (ci: ColumnInfoLike) => ESColumnMap(SoQLType.typesByName(TypeName(ci.typeName))) }
      row.foldLeft(Map.empty[String, Any]) { (acc, x) =>
        x match {
          case (colId, cell) =>
            acc + (toESColumnName(schema(colId)).toString -> esColumnMap(colId).toES(cell))
        }
      }
    }

    def rowId(columnInfo: ColumnInfoLike, row: Row[SoQLValue]): Option[RowId] = {
      val colId = columnInfo.systemId
      val cell = row(colId)
      val esColumnMap = ESColumnMap(SoQLType.typesByName(TypeName(columnInfo.typeName)))
      esColumnMap.toES(cell) match {
        case jv: JNumber => Some(new RowId(jv.toLong))
        case x => None
      }
    }
  }
}
