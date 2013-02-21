package com.socrata.es.rows

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.rows.ESColumnMap
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.Row

trait Converter[CV] {
  def toRow(schema: ColumnIdMap[ColumnInfo], row: Row[CV]): Map[String, Any]
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
  }
}
