package com.socrata.soql.adapter.elasticsearch

import com.socrata.soql.adapter.{NotImplementedException, XlateCtx}
import com.socrata.soql.functions.{SoQLFunctions}
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.typed.FunctionCall
import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.environment.FunctionName

object ESScriptFunction {

  def apply(functionName: FunctionName) = funMap.get(functionName)

  private val funMap = Map(
    IsNull.name -> isNull _,
    IsNotNull.name -> isNotNull _,
    Not.name -> not _,
    UnaryMinus.name -> unaryMinus _,
    TextToNumber.name -> textToNumber _,
    NumberToText.name -> numberToText _,
    Between.name -> between _,
    NotBetween.name -> notBetween _,
    In.name -> in _,
    NotIn.name -> notIn _,

    Eq.name -> infix _,
    EqEq.name -> infix _,
    And.name -> infix _,
    Or.name -> infix _,
    BinaryMinus.name -> infix _,
    BinaryPlus.name -> infix _,
    Gt.name -> infix _,
    Gte.name -> infix _,

    Lt.name -> infix _,
    Lte.name -> infix _,
    Neq.name -> infix _,
    BangEq.name -> infix _,
    Concat.name -> infix _
  )

  private def isNull(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    fn.parameters match {
      case (col: ColumnRef[_]) :: Nil =>
        ("doc['%s'].empty".format(col.column.name), xlateCtx)
      case _  => throw new NotImplementedException("", fn.position)
    }
  }

  private def isNotNull(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    fn.parameters match {
      case (col: ColumnRef[_]) :: Nil =>
        ("!doc['%s'].empty".format(col.column.name), xlateCtx)
      case _  => throw new NotImplementedException("", fn.position)
    }
  }

  private def not(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    fn.parameters match {
      case param :: Nil =>
        val (child, childCtx) = ESCoreExpr(param).toScript(xlateCtx, level+1)
        ("(!%s)".format(child), childCtx)
      case _  => throw new NotImplementedException("", fn.position)
    }
  }

  private def unaryMinus(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val (child, childCtx) = ESCoreExpr(fn.parameters.head).toScript(xlateCtx, level+1)
    ("-(%s)".format(child), childCtx)
  }

  private def textToNumber(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val (child, childCtx) = ESCoreExpr(fn.parameters.head).toScript(xlateCtx, level+1)
    ("parseFloat(%s)".format(child), childCtx + requireJS)
  }

  private def numberToText(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val (child, childCtx) = ESCoreExpr(fn.parameters.head).toScript(xlateCtx, level+1)
    ("(%s).toString()".format(child), childCtx + requireJS)
  }

  private def between(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val children = fn.parameters.map(ESCoreExpr(_).toScript(xlateCtx, level+1))
    val scriptedParams = children.map(x => x._1)
    val childrenCtx = children.foldLeft(xlateCtx) { (x, y) => x ++ y._2}

    ("(%1$s >= %2$s && %1$s <= %3$s)".format(scriptedParams(0), scriptedParams(1), scriptedParams(2)), childrenCtx)
  }

  private def notBetween(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Between, fn.function.bindings), fn.parameters)
    val (child, childCtx) = ESCoreExpr(deNegFn).toScript(xlateCtx, level+1)
    ("(!%s)".format(child), childCtx)
  }

  private def in(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {

    val (lhs, lhsCtx) = ESCoreExpr(fn.parameters.head).toScript(xlateCtx, level+1)
    val rhss = fn.parameters.tail.map(ESCoreExpr(_).toScript(xlateCtx, level+1))
    val childrenCtx = rhss.foldLeft(lhsCtx) { (accCtx, rhs) => accCtx ++ rhs._2 }
    val script = rhss.map( rhs => "(%s == %s)".format(lhs, rhs._1)).mkString("(", " || ", ")")
    (script, childrenCtx)
  }

  private def notIn(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.In, fn.function.bindings), fn.parameters)
    val (child, childCtx) = ESCoreExpr(deNegFn).toScript(xlateCtx, level+1)
    ("(!%s)".format(child), childCtx)
  }

  private def infix(fn: FunctionCall[_], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val children = fn.parameters.map(ESCoreExpr(_).toScript(xlateCtx, level+1))
    val scriptedParams = children.map(x => x._1)
    val childrenCtx = children.foldLeft(xlateCtx) { (x, y) => x ++ y._2}

    scriptedParams match {
      case lhs :: rhs :: Nil =>
        ("(%s %s %s)".format(lhs, scriptFnMap(fn.function.name), rhs), childrenCtx)
      case _  => throw new NotImplementedException("", fn.position)
    }
  }

  private val requireJS = (XlateCtx.ESLang -> "js")

  private val scriptFnMap = Map(
    SoQLFunctions.Eq.name -> "==",
    SoQLFunctions.EqEq.name -> "==",
    SoQLFunctions.And.name -> "&&",
    SoQLFunctions.Or.name -> "||",
    SoQLFunctions.Concat.name -> "+",

    SoQLFunctions.Gt.name -> ">",
    SoQLFunctions.Gte.name -> ">=",
    SoQLFunctions.Lt.name -> "<",
    SoQLFunctions.Lte.name -> "<=",
    SoQLFunctions.Neq.name -> "!=",
    SoQLFunctions.BangEq.name -> "!=",

    SoQLFunctions.TimesNumNum.name -> "*",
    SoQLFunctions.DivNumNum.name -> "/",
    SoQLFunctions.BinaryMinus.name -> "-",
    SoQLFunctions.BinaryPlus.name -> "+"
  )
}
