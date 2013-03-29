package com.socrata.es.soql


import util.parsing.input.Position

class SoQLAdapterException(val message: String, val position: Position) extends Exception(message + "\n" + position.longString)

class NotImplementedException(m: String, p: Position) extends SoQLAdapterException(m, p)