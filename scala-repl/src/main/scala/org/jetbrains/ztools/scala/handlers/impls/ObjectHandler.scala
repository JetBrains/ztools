package org.jetbrains.ztools.scala.handlers.impls

import org.apache.commons.lang.exception.ExceptionUtils
import org.jetbrains.ztools.scala.core.{Loopback, ResNames}
import org.jetbrains.ztools.scala.handlers.HandlerManager
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo
import org.jetbrains.ztools.scala.reference.ReferenceManager

import scala.collection.mutable
import scala.reflect.api.JavaUniverse

class ObjectHandler(val stringSizeLimit: Int,
                    val manager: HandlerManager,
                    val referenceManager: ReferenceManager,
                    val timeout: Int) extends AbstractTypeHandler {
  private val INACCESSIBLE = ScalaVariableInfo(isAccessible = false, isLazy = false, null, null, null, null)
  val ru: JavaUniverse = scala.reflect.runtime.universe
  val mirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)

  case class ReflectionProblem(e: Throwable, symbol: String, var count: Int)

  val problems: mutable.Map[String, ReflectionProblem] = mutable.Map[String, ReflectionProblem]()

  override def accept(obj: Any): Boolean = true

  override def handle(scalaInfo: ScalaVariableInfo, loopback: Loopback, depth: Int): mutable.Map[String, Any] =
    withJsonObject { result =>
      val obj = scalaInfo.value

      if (obj == null) {
        return result
      }
      if (depth <= 0) {
        result += (ResNames.VALUE -> obj.toString.take(stringSizeLimit))
        return result
      }

      val fields = listAccessibleProperties(scalaInfo)
      if (fields.isEmpty) {
        result += (ResNames.VALUE -> obj.toString.take(stringSizeLimit))
        return result
      }

      val resolvedFields = mutable.Map[String, Any]()
      result += (ResNames.VALUE -> resolvedFields)

      val startTime = System.currentTimeMillis()

      fields.foreach { field =>
        if (checkTimeoutError(field.name, startTime, timeout)) {
          return result
        }

        if (field.ref != null && field.ref != field.path) {
          resolvedFields += (field.name -> (mutable.Map[String, Any]() += (ResNames.REF -> field.ref)))
        } else {
          resolvedFields += (field.name -> manager.handleVariable(field, loopback, depth - 1))
        }
      }

      result
    }


  override def getErrors: List[String] = problems.map(x =>
    f"Reflection error for ${x._2.symbol} counted ${x._2.count}.\n" +
      f"Error message: ${ExceptionUtils.getMessage(x._2.e)}\n " +
      f"Stacktrace:${ExceptionUtils.getStackTrace(x._2.e)}").toList ++ super.getErrors

  private def listAccessibleProperties(info: ScalaVariableInfo): List[ScalaVariableInfo] = {
    val instanceMirror = mirror.reflect(info.value)
    val instanceSymbol = instanceMirror.symbol
    val members = instanceSymbol.toType.members
    members.map {
      symbol => get(instanceMirror, symbol, info.path)
    }.filter(_.isAccessible).toList
  }

  private def get(instanceMirror: ru.InstanceMirror, symbol: ru.Symbol, path: String): ScalaVariableInfo = {
    if (!problems.contains(path))
      try {
        // is public property
        if (!symbol.isMethod && symbol.isTerm && symbol.asTerm.getter.isPublic) {
          val term = symbol.asTerm
          val f = instanceMirror.reflectField(term)
          val fieldPath = s"$path.${term.name.toString.trim}"
          val value = f.get
          val tpe = term.typeSignature.toString
          return ScalaVariableInfo(isAccessible = tpe != "<notype>", isLazy = term.isLazy, value, tpe,
            fieldPath, referenceManager.getRef(value, fieldPath))
        }
      } catch {
        case e: Throwable => problems(path) = ReflectionProblem(e, symbol.toString, 1)
      }
    else
      problems(path).count += 1

    INACCESSIBLE
  }
}