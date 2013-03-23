package com.socrata.soql.adapter.elasticsearch

import com.socrata.es.facet.Facet
import com.socrata.soql.typed._
import com.rojoma.json.ast._
import com.socrata.soql.adapter.{XlateCtx, NotImplementedException, SoqlAdapter}
import com.socrata.rows.ESGateway
import com.socrata.soql.collection.{OrderedSet, OrderedMap}
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.types._
import com.socrata.soql.SoQLAnalyzer
import com.rojoma.json.ast.JObject
import com.rojoma.json.ast.JArray
import com.socrata.soql.functions.{SoQLFunctions, SoQLTypeInfo, SoQLFunctionInfo, MonomorphicFunction}
import com.socrata.soql.parsing.SoQLPosition
import com.rojoma.json.ast.JString
import com.socrata.es.facet.{ColumnsFacet, TermsStatsFacet, ColumnsFacetType, TermsStatsFacetType}

class ESQuery(val resource: String, val esGateway: ESGateway, defaultLimit: Option[BigInt] = Some(1000),
              context: Option[DatasetContext[SoQLType]] = None) extends SoqlAdapter[String] {

  import ESQuery._

  private lazy val dsCtx = context.getOrElse(esGateway.getDataContext())

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

    val whereAndSearch = andWhereSearch(analysis.where, analysis.search)
    val esFilter = whereAndSearch.map(toESFilter(_, xlateCtx, canScript = true))
    val esFilterOrQuery = esFilter.map { filter =>
    // A top filter apply independently of facets
    // Must wrap in a query in order to affect facets.
    // filtered query must have query but we don't use it because it cannot handle and/or.
    // We fool it be puting match_all there.
      if (analysis.isGrouped) JObject(Map("filtered"-> JObject(OrderedMap("query" -> JObject1("match_all", JObject0), "filter" -> filter))))
      else filter
    }

    // For group by, we don't want any individual rows to be returned.  Setting limit to 0 accomplish that.
    val esLimit =
      if (analysis.isGrouped) Some(toESLimit(0))
      else (analysis.limit ++ defaultLimit).headOption.map(toESLimit)

    val esOrder =
      if (analysis.isGrouped) None
      else analysis.orderBy.map(toESOrderBy)

    val esGroup =
      if (analysis.isGrouped) Some(toESGroupBy(analysis.groupBy.getOrElse(Seq.empty[CoreExpr[SoQLType]]),
        analysis.orderBy, analysis.selection, analysis.offset, analysis.limit))
      else None

    val esObjectPropsSomeValuesMayBeEmpty =
      OrderedMap(
        (if (analysis.isGrouped) "query" else "filter") -> esFilterOrQuery,
        "sort" -> esOrder,
        "facets" -> esGroup,
        "from" -> analysis.offset.map(toESOffset),
        "size" -> esLimit
      )

    val esObjectProps = esObjectPropsSomeValuesMayBeEmpty.collect { case (k, v) if v.isDefined => (k, v.get) }
    val esObject = JObject(esObjectProps)
    Tuple2(esObject.toString, analysis) //.replace("\n", " ")
  }

  private def andWhereSearch(whereOpt: Option[CoreExpr[SoQLType]], searchOpt: Option[String]): Option[CoreExpr[SoQLType]] = {

    val lostPos = new SoQLPosition(0, 0, "", 0)

    def searchToExpr(search: String) = {
      FunctionCall[SoQLType](
        ESFunction.Search,
        Seq(StringLiteral(search, SoQLType)(lostPos).asInstanceOf[CoreExpr[SoQLType]]))(lostPos, lostPos)
    }

    def and(lhs: CoreExpr[SoQLType], rhs: CoreExpr[SoQLType]) = {
      val fAnd = new MonomorphicFunction(SoQLFunctions.And.name, Seq(SoQLBoolean, SoQLBoolean), None, SoQLBoolean)
      FunctionCall[SoQLType](fAnd, Seq(lhs, rhs))(lostPos, lostPos)
    }

    (whereOpt, searchOpt) match {
      case (Some(where), None) =>
        whereOpt
      case (None, Some(search)) =>
        Some(searchToExpr(search))
      case (Some(where), Some(search)) =>
        Some(and(where, searchToExpr(search)))
      case (None, None) =>
        None
    }
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

    val lim = if (limit.isDefined) limit else defaultLimit
    groupBys.size match {
      case 1 =>
        val facet = TermsStatsFacet(groupBys, orderBys, cols, offset, lim)
        facet.toJObject
      case 0 => // implied grouping like select count(x)
        toImpliedGroupFacet(cols, orderBys, offset, lim)
      case n =>
        val facet = ColumnsFacet(groupBys, orderBys, cols, offset, lim)
        facet.toJObject
    }
  }

  private def toImpliedGroupFacet(cols: OrderedMap[ColumnName, CoreExpr[SoQLType]],
                            orderBys: Option[Seq[OrderBy[SoQLType]]],
                            offset: Option[BigInt] = None, limit: Option[BigInt] = None): JObject = {

    val esGroupBys = cols.collect {
      case (columnName, aggExpr) =>
        Facet(columnName, aggExpr).toJObject
    }

    // Merge everything into one row.  If there is a single gropu, we could have skipped the merge.
    esGroupBys.fold(JObject(Map.empty))((jo1, jo2) => JObject(jo1.toMap ++ jo2.toMap))
  }

  private def toESFilter(filter: CoreExpr[SoQLType], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    ESCoreExpr(filter).toFilter(xlateCtx, level, canScript)
  }
}

object ESQuery {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[ESQuery])

  private val JObject0 = JObject(Map.empty)

  def JObject1(k: String, v: JValue): JObject = JObject(Map(k -> v))

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  val AggregateFunctions: Set[com.socrata.soql.functions.Function[SoQLType]] =
    Set(SoQLFunctions.Max, SoQLFunctions.Min, SoQLFunctions.Count, SoQLFunctions.Sum, SoQLFunctions.Avg)

  val AggregateFunctionTermsStatsMap = Map(
    SoQLFunctions.Count.name -> "count",
    SoQLFunctions.Sum.name -> "total",
    SoQLFunctions.Min.name -> "min",
    SoQLFunctions.Max.name -> "max",
    SoQLFunctions.Avg.name -> "mean")

  private def checkKnownAggregateFuns {
    val KnownAggregateFuns = SoQLFunctions.allFunctions.filter(_.isAggregate)
    KnownAggregateFuns.foreach { f =>
      if (!(AggregateFunctions.contains(f) && AggregateFunctionTermsStatsMap.isDefinedAt(f.name)))
        log.warn(s"aggregate function ${f} is not implemented.")
    }
  }

  checkKnownAggregateFuns
}
