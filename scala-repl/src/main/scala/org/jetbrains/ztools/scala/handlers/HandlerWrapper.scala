package org.jetbrains.ztools.scala.handlers

import org.jetbrains.ztools.scala.core.{Loopback, ResNames, TypeHandler}
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo

import scala.collection.mutable

class HandlerWrapper(val handler: TypeHandler) {
  def accept(info: ScalaVariableInfo): Boolean = info.isLazy || handler.accept(info.value)

  def handle(scalaInfo: ScalaVariableInfo, loopback: Loopback, depth: Int): mutable.Map[String, Any] = {
    val data = if (scalaInfo.isLazy) {
      mutable.Map[String, Any](ResNames.LAZY -> true)
    }
    else {
      handler.handle(scalaInfo, loopback, depth: Int)
    }

    data.put(ResNames.TYPE, calculateType(scalaInfo))
    data
  }

  private def calculateType(scalaInfo: ScalaVariableInfo): String = {
    if (scalaInfo.tpe != null)
      return scalaInfo.tpe

    if (scalaInfo.value != null)
      scalaInfo.value.getClass.getCanonicalName
    else
      null
  }
}