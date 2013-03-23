package com.socrata.es.facet

import scala.language.existentials
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JString, JObject, JValue}
import com.socrata.soql.typed.{FunctionCall, CoreExpr, ColumnRef}
import com.socrata.soql.types.{SoQLType, SoQLText, SoQLNumber}
import com.socrata.soql.adapter.elasticsearch.FacetName
import com.socrata.soql.environment.{FunctionName, ColumnName}

object StatisticalFacet {

  /**
   * select count(*) relies on the system column :id to work
   * in order to save us from dealing with yet another special case
   */
  val CountStar = ":id"

  implicit object jcodec extends JsonCodec[StatisticalFacet] {

    def encode(facet: StatisticalFacet): JValue = {
      val colName = facet.columnRef.map(_.column.name).getOrElse(CountStar)
      val facetName = FacetName(StatisticalFacetType,  StatisticalFacetType.groupKey, colName)
      JObject(Map(facetName.toString ->
        JObject(Map("statistical" ->
          JObject(Map("field" -> JString(colName)))
        ))))
    }

    def decode(in: JValue): Option[StatisticalFacet] = None // no need for decode
  }
}

case class StatisticalFacet(val columnRef: Option[ColumnRef[_]]) extends Facet {

  protected def toJValue = JsonCodec.toJValue(this)
}
