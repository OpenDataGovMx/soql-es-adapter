package com.socrata.es.facet

import scala.language.existentials
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast.{JString, JObject, JValue}
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.es.soql.query.FacetName

object TermsFacet {
  implicit object jcodec extends JsonCodec[TermsFacet] {

    def encode(facet: TermsFacet): JValue = {
      val colName = facet.columnRef.column.name
      val order =
        if (facet.aggregateFunctionName == SoQLFunctions.Max.name) "reverse_term"
        else "term"

      val facetName = FacetName(TermsFacetType, "_" + facet.aggregateFunctionName.name, colName)
      JObject(Map(facetName.toString ->
        JObject(Map("terms" ->
          JObject(Map(
            "field" -> JString(colName),
            "order" -> JString(order)
          ))
        ))))
    }

    def decode(in: JValue): Option[TermsFacet] = None // no need for decode
  }
}

case class TermsFacet(val columnRef: ColumnRef[_], val aggregateFunctionName: FunctionName) extends Facet {

  protected def toJValue = JsonCodec.toJValue(this)
}