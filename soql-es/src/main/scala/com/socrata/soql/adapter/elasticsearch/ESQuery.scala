package com.socrata.soql.adapter.elasticsearch

import com.socrata.soql.typed._
import com.rojoma.json.ast._
import com.socrata.soql.adapter.{SoQLAdapterException, XlateCtx, NotImplementedException, SoqlAdapter}
import com.socrata.rows.ESGateway
import com.socrata.soql.collection.{OrderedSet, OrderedMap}
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.types._
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.typed.OrderBy
import com.rojoma.json.ast.JObject
import com.rojoma.json.ast.JArray
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.typed.FunctionCall
import com.rojoma.json.ast.JString
import com.socrata.soql.functions.{SoQLFunctions, SoQLTypeInfo, SoQLFunctionInfo, MonomorphicFunction}

class ESQuery(val resource: String, val esGateway: ESGateway, defaultLimit: Option[BigInt] = Some(1000)) extends SoqlAdapter[String] {

  import ESQuery._

  private lazy val dsCtx = esGateway.getDataContext()

  def full(soql: String) = toQuery(dsCtx, soql)

  def select(selection : OrderedMap[ColumnName, CoreExpr[SoQLType]]) = ""

  def where(filter: CoreExpr[SoQLType], xlateCtx: Map[XlateCtx.Value, AnyRef] = Map.empty) =
    toESFilter(filter, xlateCtx, canScript = true).toString()

  def orderBy(orderBys: Seq[OrderBy[SoQLType]]) =
    toESOrderBy(orderBys).toString()

  def groupBy(groupBys: Seq[CoreExpr[SoQLType]], cols: OrderedMap[ColumnName, CoreExpr[SoQLType]]) =
    toESGroupBy(groupBys, None, cols).toString

  def offset(offset: Option[BigInt]) = offset.map(toESOffset).toString

  def limit(limit: Option[BigInt]) = limit.map(toESLimit).toString

  private def toQuery(datasetCtx: DatasetContext[SoQLType], soql: String): Tuple2[String, SoQLAnalyzer[SoQLType]#Analysis] = {
    implicit val ctx: DatasetContext[SoQLType] = datasetCtx

    val analysis = analyzer.analyzeFullQuery(soql)
    val xlateCtx: Map[XlateCtx.Value, AnyRef] = Map(XlateCtx.LowercaseStringLiteral -> Boolean.box(true))
    val esFilter = analysis.where.map(toESFilter(_, xlateCtx, canScript = true))
    val esFilterOrQuery = esFilter.map { filter =>
    // A top filter apply independently of facets
    // Must wrap in a query in order to affect facets.
    // filtered query must have query but we don't use it because it cannot handle and/or.
    // We fool it be puting match_all there.
      if (analysis.groupBy.isDefined) JObject(Map("filtered"-> JObject(OrderedMap("query" -> JObject1("match_all", JObject0), "filter" -> filter))))
      else filter
    }

    // For group by, we don't want any individual rows to be returned.  Setting limit to 0 accomplish that.
    val esLimit =
      if (analysis.isGrouped) Some(toESLimit(0))
      else (analysis.limit ++ defaultLimit).headOption.map(toESLimit)

    val esOrder =
      if (analysis.isGrouped) None
      else analysis.orderBy.map(toESOrderBy)

    val esObjectPropsSomeValuesMayBeEmpty =
      OrderedMap(
        (if (analysis.groupBy.isDefined) "query" else "filter") -> esFilterOrQuery,
        "sort" -> esOrder,
        "facets" -> analysis.groupBy.map(toESGroupBy(_, analysis.orderBy, analysis.selection, analysis.offset, analysis.limit)),
        "from" -> analysis.offset.map(toESOffset),
        "size" -> esLimit
      )

    val esObjectProps = esObjectPropsSomeValuesMayBeEmpty.collect { case (k, v) if v.isDefined => (k, v.get) }
    val esObject = JObject(esObjectProps)
    Tuple2(esObject.toString, analysis) //.replace("\n", " ")
  }

  private def toESOffset(offset: BigInt) = JNumber(offset)

  private def toESLimit(limit: BigInt) = JNumber(limit)

  private def toESOrderBy(orderBys: Seq[OrderBy[SoQLType]]): JValue = {
    JArray(orderBys.map { ob =>
      ob.expression match {
        case col: ColumnRef[_] =>
          JObject(Map(col.column.name -> JString(if (ob.ascending) "asc" else "desc")))
        case _ =>
          throw new NotImplementedException("Sort by expressiion is not implemented", ob.expression.position)
      }
    })
  }

  private def toESGroupBy(groupBys: Seq[CoreExpr[SoQLType]],
                          orderBys: Option[Seq[OrderBy[SoQLType]]],
                          cols: OrderedMap[ColumnName, CoreExpr[SoQLType]],
                          offset: Option[BigInt] = None, limit: Option[BigInt] = None): JObject = {

    if (groupBys.size > 1) toESGroupByColumns(groupBys, orderBys, cols, offset, limit)
    else toESGroupByTermsStats(groupBys, cols, offset, limit)
  }

  private def toESGroupByTermsStats(groupBys: Seq[CoreExpr[SoQLType]], cols: OrderedMap[ColumnName, CoreExpr[SoQLType]],
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
            if (limit.isEmpty && offset.isEmpty) defaultLimit.getOrElse(0)
            else if (limit.isDefined && offset.isEmpty) limit.get
            else if (offset.isDefined && limit.isEmpty) offset.get + defaultLimit.getOrElse(1)
            else limit.get + offset.get
          val facetKeyVal = JObject(Map(
            "key_field" -> JString(col.column.name), facetVal._1 -> facetVal._2,
            "order" -> JString("term"), // TODO - allow different orderings
            "size" -> JNumber(size)
          ))
          val termsStats = JObject(Map("terms_stats" -> facetKeyVal))
          val facetName = FacetName(col.column.name, aggregateValue.column.name)
          val facet = (facetName.toString(), termsStats)
          facet
        }.toMap)
    }
    if (esGroupBys.size > 1) throw new NotImplementedException("Cannot handle multiple groups", groupBys(1).position)
    esGroupBys.head
  }

  private def toESGroupByColumns(groupBys: Seq[CoreExpr[SoQLType]],
                          orderBys: Option[Seq[OrderBy[SoQLType]]],
                          cols: OrderedMap[ColumnName, CoreExpr[SoQLType]],
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
              (AggregateFunctions.contains(function)) && column.name == aggregateValue.column.name =>
                JString(":%s%s".format(function.name.name, (if (asc) "" else " desc")))
            }
        }

        val facetMap = OrderedMap( // ES columns facet needs orders to be specified after keys.  Therefore, we use ordered map.
          "key_fields" -> keys,
          facetVal._1 -> facetVal._2,
          "size" -> JNumber(limit.getOrElse(defaultLimit.getOrElse(BigInt(10)))),
          "from" -> JNumber(offset.getOrElse(BigInt(0)))
        )
        val facetWithSortMap = orders match {
          case Some(obs) => facetMap + ("orders" -> JArray(obs))
          case None => facetMap
        }
        val facetKeyVal = JObject(facetWithSortMap)
        val termsStats = JObject(Map("columns" -> facetKeyVal))
        val facetName = FacetName("_multi", aggregateValue.column.name)
        val facet = (facetName.toString(), termsStats)
        facet
      }.toMap)
  }

  private def toESFilter(filter: CoreExpr[SoQLType], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    ESCoreExpr(filter).toFilter(xlateCtx, level, canScript)
  }
}

object ESQuery {

  private val JObject0 = JObject(Map.empty)

  def JObject1(k: String, v: JValue): JObject = JObject(Map(k -> v))

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  val AggregateFunctions: Set[com.socrata.soql.functions.Function[SoQLType]] =
    Set(SoQLFunctions.Max, SoQLFunctions.Min, SoQLFunctions.Count, SoQLFunctions.Sum, SoQLFunctions.Avg)
}
