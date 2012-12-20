package com.socrata.soql.adapter.elasticsearch

import com.socrata.soql.typed.{ColumnRef, FunctionCall}
import com.socrata.soql.adapter.{NotImplementedException, XlateCtx}
import com.socrata.soql.functions.{SoQLFunctions, MonomorphicFunction}

object ESScriptFunction {

  def isNull[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    fn.parameters match {
      case (col: ColumnRef[_]) :: Nil =>
        ("doc['%s'].empty".format(col.column.name), xlateCtx)
      case _  => throw new NotImplementedException("", fn.position)
    }
  }

  def isNotNull[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    fn.parameters match {
      case (col: ColumnRef[_]) :: Nil =>
        ("!doc['%s'].empty".format(col.column.name), xlateCtx)
      case _  => throw new NotImplementedException("", fn.position)
    }
  }

  def not[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    fn.parameters match {
      case param :: Nil =>
        val (child, childCtx) = ESCoreExpr(param).toScript(xlateCtx, level+1)
        ("(!%s)".format(child), childCtx)
      case _  => throw new NotImplementedException("", fn.position)
    }
  }

  def unaryMinus[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val (child, childCtx) = ESCoreExpr(fn.parameters.head).toScript(xlateCtx, level+1)
    ("-(%s)".format(child), childCtx)
  }

  def textToNumber[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val (child, childCtx) = ESCoreExpr(fn.parameters.head).toScript(xlateCtx, level+1)
    ("parseFloat(%s)".format(child), childCtx + requireJS)
  }

  def numberToText[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val (child, childCtx) = ESCoreExpr(fn.parameters.head).toScript(xlateCtx, level+1)
    ("(%s).toString()".format(child), childCtx + requireJS)
  }

  def between[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val children = fn.parameters.map(ESCoreExpr(_).toScript(xlateCtx, level+1))
    val scriptedParams = children.map(x => x._1)
    val childrenCtx = children.foldLeft(xlateCtx) { (x, y) => x ++ y._2}

    ("(%1$s >= %2$s && %1$s <= %3$s)".format(scriptedParams(0), scriptedParams(1), scriptedParams(2)), childrenCtx)
  }

  def notBetween[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.Between, fn.function.bindings), fn.parameters)
    val (child, childCtx) = ESCoreExpr(deNegFn).toScript(xlateCtx, level+1)
    ("(!%s)".format(child), childCtx)
  }

  def in[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {

    val (lhs, lhsCtx) = ESCoreExpr(fn.parameters.head).toScript(xlateCtx, level+1)
    val rhss = fn.parameters.tail.map(ESCoreExpr(_).toScript(xlateCtx, level+1))
    val childrenCtx = rhss.foldLeft(lhsCtx) { (accCtx, rhs) => accCtx ++ rhs._2 }
    val script = rhss.map( rhs => "(%s == %s)".format(lhs, rhs._1)).mkString("(", " || ", ")")
    (script, childrenCtx)
  }

  def notIn[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
    Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    val deNegFn = FunctionCall(MonomorphicFunction(SoQLFunctions.In, fn.function.bindings), fn.parameters)
    val (child, childCtx) = ESCoreExpr(deNegFn).toScript(xlateCtx, level+1)
    ("(!%s)".format(child), childCtx)
  }

  def infix[T](fn: FunctionCall[T], xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0):
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
