package com.socrata.es.facet

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.ast._
import com.socrata.soql.typed._
import com.socrata.soql.collection.{OrderedSet, OrderedMap}
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.typed.OrderBy
import com.socrata.soql.typed.FunctionCall
import com.rojoma.json.ast.JObject
import com.rojoma.json.ast.JString
import com.socrata.soql.functions.MonomorphicFunction
import com.rojoma.json.ast.JArray
import com.socrata.soql.types.SoQLAnalysisType
import com.socrata.soql.environment.ColumnName
import com.socrata.es.soql.NotImplementedException
import com.socrata.es.soql.query.{FacetName, ESQuery}

object ColumnsFacet {

  implicit object jcodec extends JsonCodec[ColumnsFacet] {

    def encode(facet: ColumnsFacet): JValue = {
      toESGroupByColumns(facet.groupBys, facet.orderBys, facet.cols, facet.offset, facet.limit)
    }

    def decode(in: JValue): Option[ColumnsFacet] = None // no need for decode

    private def toESGroupByColumns(groupBys: Seq[CoreExpr[SoQLAnalysisType]],
                                   orderBys: Option[Seq[OrderBy[SoQLAnalysisType]]],
                                   cols: OrderedMap[ColumnName, CoreExpr[SoQLAnalysisType]],
                                   offset: Option[BigInt] = None, limit: Option[BigInt] = None): JObject = {

      val keys = JArray(groupBys.collect { // ignore expression that aren't simple column
        case col: ColumnRef[_] => JString(col.column.name)
      })

      val aggregateColumns = cols.foldLeft(OrderedSet.empty[ColumnRef[_]]) { (aggregateCols, outputCol) => outputCol match {
        case (columnName, expr) =>
          expr match {
            case FunctionCall(function, (arg : ColumnRef[_]) :: Nil) if function.isAggregate =>
              // ColumnRef contains position info which prevents us from directly using ColumnRef equal.
              if (aggregateCols.find(_.column == arg.column).isDefined) aggregateCols
              else aggregateCols + arg
            case FunctionCall(function, arg) if function.isAggregate =>
              // require facet script value
              throw new NotImplementedException("Aggregate on expression not implemented", expr.position)
            case _ =>
              // should never happen
              aggregateCols
          }
      }}

      JObject(aggregateColumns.map { aggregateValue =>
        val facetVal = ("value_field", JString(aggregateValue.column.name))

        val orders = orderBys.map { obs =>
          obs.collect {
            case OrderBy(ColumnRef(column, typ), asc, nullLast) =>
              JString("%s%s".format(column.name, (if (asc) "" else " desc")))
            case OrderBy(FunctionCall(MonomorphicFunction(function, _), ColumnRef(column, typ) :: Nil), asc, nullLast) if
            (ESQuery.AggregateFunctions.contains(function)) && column.name == aggregateValue.column.name =>
              JString(":%s%s".format(function.name.name, (if (asc) "" else " desc")))
          }
        }

        val facetMap = OrderedMap( // ES columns facet needs orders to be specified after keys.  Therefore, we use ordered map.
          "key_fields" -> keys,
          facetVal._1 -> facetVal._2,
          "size" -> JNumber(limit.getOrElse(BigInt(10))),
          "from" -> JNumber(offset.getOrElse(BigInt(0)))
        )
        val facetWithSortMap = orders match {
          case Some(obs) => facetMap + ("orders" -> JArray(obs))
          case None => facetMap
        }
        val facetKeyVal = JObject(facetWithSortMap)
        val termsStats = JObject(Map("columns" -> facetKeyVal))
        val facetName = FacetName(ColumnsFacetType, ColumnsFacetType.groupKey, aggregateValue.column.name)
        val facet = (facetName.toString(), termsStats)
        facet
      }.toMap)
    }
  }
}

case class ColumnsFacet(
  val groupBys: Seq[CoreExpr[SoQLAnalysisType]],
  val orderBys: Option[Seq[OrderBy[SoQLAnalysisType]]],
  val cols: OrderedMap[ColumnName, CoreExpr[SoQLAnalysisType]],
  val offset: Option[BigInt] = None,
  val limit: Option[BigInt] = None) extends Facet {

  protected def toJValue = JsonCodec.toJValue(this)
}
