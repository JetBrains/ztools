package org.jetbrains.ztools.scala.handlers.impls

import org.jetbrains.ztools.scala.core.{Loopback, ResNames}
import org.jetbrains.ztools.scala.handlers.HandlerManager
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo
import org.jetbrains.ztools.scala.reference.ReferenceManager

import scala.collection.mutable
import scala.reflect.api.JavaUniverse

class ObjectHandler(val stringSizeLimit: Int,
                    val manager: HandlerManager,
                    val referenceManager: ReferenceManager) extends AbstractTypeHandler {
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
      val resolvedFields = mutable.Map[String, Any]()
      fields.foreach { field =>
        if (field.ref != null && field.ref != field.path) {
          resolvedFields += (field.name -> (mutable.Map[String, Any]() += (ResNames.REF -> field.ref)))
        } else {
          resolvedFields += (field.name -> manager.handleVariable(field, loopback, depth - 1))
        }
      }

      if (resolvedFields.nonEmpty)
        result += (ResNames.VALUE -> resolvedFields)
      else
        result += (ResNames.VALUE -> obj.toString.take(stringSizeLimit))
    }

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