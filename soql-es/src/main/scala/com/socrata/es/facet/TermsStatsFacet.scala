package com.socrata.es.facet

import com.socrata.soql.typed.{FunctionCall, ColumnRef, OrderBy, CoreExpr}
import com.socrata.soql.types.{SoQLType, SoQLNumber}
import com.socrata.soql.collection.{OrderedSet, OrderedMap}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.ast.{JValue, JNumber, JString, JObject}
import com.rojoma.json.codec.JsonCodec
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.es.soql.NotImplementedException
import com.socrata.es.soql.query.{FacetName, ESQuery}

object TermsStatsFacet {

  implicit object jcodec extends JsonCodec[TermsStatsFacet] {

    def encode(facet: TermsStatsFacet): JValue = {
      toESGroupByTermsStats(facet.groupBys, facet.orderBys, facet.cols, facet.offset, facet.limit)
    }

    def decode(in: JValue): Option[TermsStatsFacet] = None // no need for decode

    private def toESGroupByTermsStats(groupBys: Seq[CoreExpr[SoQLType]], orderBys: Option[Seq[OrderBy[SoQLType]]],
                                      cols: OrderedMap[ColumnName, CoreExpr[SoQLType]],
                                      offset: Option[BigInt] = None, limit: Option[BigInt] = None): JObject = {

      val esGroupBys = groupBys.collect { // ignore expression that aren't simple column
        case col: ColumnRef[_] =>
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
            val facetVal = aggregateValue.typ match {
              case SoQLNumber =>
                ("value_field", JString(aggregateValue.column.name))
              case _ =>
                // terms stat only works on numeric fields.  Use value script to at least get sensible result for count(text)
                ("value_script", JString("doc[%s].empty ? 0 : 1".format(JString(aggregateValue.column.name))))
            }
            // Make some sense out of optional offset, limit and default limit.
            val size: BigInt =
              if (limit.isEmpty && offset.isEmpty) 0
              else if (limit.isDefined && offset.isEmpty) limit.get
              else if (offset.isDefined && limit.isEmpty) offset.get + 1
              else limit.get + offset.get
            val facetKeyVal = JObject(Map(
              "key_field" -> JString(col.column.name), facetVal._1 -> facetVal._2,
              "order" -> termsStatsOrderBy(orderBys),
              "size" -> JNumber(size)
            ))
            val termsStats = JObject(Map("terms_stats" -> facetKeyVal))
            val facetName = FacetName(TermsStatsFacetType, col.column.name, aggregateValue.column.name)
            val facet = (facetName.toString(), termsStats)
            facet
          }.toMap)
      }
      if (esGroupBys.size > 1) throw new NotImplementedException("Cannot handle multiple groups", groupBys(1).position)
      esGroupBys.head
    }

    private def termsStatsOrderBy(orderBys: Option[Seq[OrderBy[SoQLType]]]): JString = {

      def ascend(asc: Boolean) = if (asc) "" else "reverse_"

      val obs = orderBys.map { obs =>
        obs.collect {
          case OrderBy(ColumnRef(column, typ), asc, nullLast) =>
            s"${ascend(asc)}term"
          case OrderBy(FunctionCall(MonomorphicFunction(function, _), ColumnRef(column, typ) :: Nil), asc, nullLast) if ESQuery.AggregateFunctions.contains(function) =>
            s"${ascend(asc)}${ESQuery.AggregateFunctionTermsStatsMap(function.name)}"
          case u => throw new NotImplementedException(s"ordered by $u", u.expression.position)
        }
      }

      obs match {
        case None => JString("term")
        case Some(h :: Nil) => JString(h)
        case Some(h :: t) =>
          // TODO: Consider ditching termsStats facet in favor of column facet or scripted facet
          throw new NotImplementedException(s"can only handle one order by expression when there is one group by",
            orderBys.get.head.expression.position)
      }
    }
  }
}

case class TermsStatsFacet(
  val groupBys: Seq[CoreExpr[SoQLType]],
  val orderBys: Option[Seq[OrderBy[SoQLType]]],
  val cols: OrderedMap[ColumnName, CoreExpr[SoQLType]],
  val offset: Option[BigInt] = None,
  val limit: Option[BigInt] = None) extends Facet {

  protected def toJValue = JsonCodec.toJValue(this)
}
