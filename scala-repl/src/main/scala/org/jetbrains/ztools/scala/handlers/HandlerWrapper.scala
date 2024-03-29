package org.jetbrains.ztools.scala.handlers

import org.jetbrains.ztools.scala.core.{Loopback, ResNames, TypeHandler}
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo

import scala.collection.mutable

class HandlerWrapper(val handler: TypeHandler, profile: Boolean) {
  def accept(info: ScalaVariableInfo): Boolean = info.isLazy || handler.accept(info.value)

  def handle(scalaInfo: ScalaVariableInfo, loopback: Loopback, depth: Int, initStartTime: Long): Any = {
    val startTime = if (initStartTime!=null)
      initStartTime
    else
      System.currentTimeMillis()

    val data = if (scalaInfo.isLazy) {
      mutable.Map[String, Any](ResNames.LAZY -> true)
    }
    else {
      val data = handler.handle(scalaInfo, loopback, depth: Int)
      if (data.keys.count(_ == ResNames.IS_PRIMITIVE) > 0) {
        return data(ResNames.VALUE)
      }
      data
    }

    data.put(ResNames.TYPE, calculateType(scalaInfo))
    if (profile)
      data.put(ResNames.TIME, System.currentTimeMillis() - startTime)

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