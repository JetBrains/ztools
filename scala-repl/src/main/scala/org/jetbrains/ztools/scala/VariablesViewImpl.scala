/**
 * Copyright 2020 Jetbrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetbrains.ztools.scala

import org.apache.commons.lang.exception.ExceptionUtils
import org.jetbrains.ztools.scala.core.{Loopback, ResNames}
import org.jetbrains.ztools.scala.handlers._
import org.jetbrains.ztools.scala.interpreter.{InterpreterHandler, ScalaVariableInfo}
import org.jetbrains.ztools.scala.reference.ReferenceManager
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import java.util.function.{Function => JFunction}
import java.util.regex.Pattern
import scala.collection.{immutable, mutable}
import scala.language.implicitConversions
import scala.tools.nsc.interpreter.IMain
import scala.util.Try

class VariablesViewImpl(val intp: IMain,
                        val timeout: Int,
                        val collectionSizeLimit: Int,
                        val stringSizeLimit: Int,
                        val blackList: List[String],
                        val whiteList: List[String] = null,
                        val filterUnitResults: Boolean,
                        val enableProfiling: Boolean,
                        val depth: Int,
                        val interpreterResCountLimit: Int = 5) extends VariablesView {
  private val interpreterHandler = new InterpreterHandler(intp)
  private val referenceManager = new ReferenceManager()

  private val touched = mutable.Map[String, ScalaVariableInfo]()

  private val handlerManager = new HandlerManager(
    collectionSizeLimit = collectionSizeLimit,
    stringSizeLimit = stringSizeLimit,
    timeout = 2000,
    referenceManager = referenceManager
  )

  //noinspection ScalaUnusedSymbol
  def getZtoolsJsonResult: String = {
    implicit val ztoolsFormats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    Serialization.write(
      Map(
        "variables" -> resolveVariables,
        "errors" -> this.errors.toList
      )
    )
  }

  override def toJson: String = {
    implicit val ztoolsFormats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    Serialization.write(resolveVariables)
  }

  override def resolveVariables: mutable.Map[String, Any] = {
    val result = mutable.Map[String, Any]()
    val startTime = System.currentTimeMillis()

    val interpreterVariablesNames = interpreterHandler.getVariableNames
    val finalNames = filterVariableNames(interpreterVariablesNames)

    var passedVariablesCount = 0
    val totalVariablesCount = finalNames.size

    if (checkTimeout(startTime, passedVariablesCount, totalVariablesCount))
      return result

    finalNames.foreach { name =>
      if (checkTimeout(startTime, passedVariablesCount, totalVariablesCount))
        return result

      passedVariablesCount += 1

      try {
        val info = interpreterHandler.getInfo(name)
        val ref = referenceManager.getRef(info.value, name)
        touched(info.path) = info

        if (ref != null && ref != info.path) {
          result += (info.path -> mutable.Map[String, Any](ResNames.REF -> ref))
        } else {
          result += info.path -> parseInfo(info, depth)
        }
      } catch {
        case t: Throwable => errors +=
          f"${ExceptionUtils.getRootCauseMessage(t)}\n${ExceptionUtils.getStackTrace(t)}"
      }
    }
    result
  }

  override def resolveVariable(path: String, deep: Int): mutable.Map[String, Any] = {
    val result = mutable.Map[String, Any]()
    val obj = touched.get(path).orNull
    if (obj.ref != null) {
      result += (ResNames.VALUE -> mutable.Map[String, Any](ResNames.REF -> obj.ref))
    } else {
      result += (ResNames.VALUE -> parseInfo(obj, depth))
    }
    result
  }

  private def parseInfo(info: ScalaVariableInfo, depth: Int): Any = {
    val loopback = new Loopback {
      override def pass(obj: Any, id: String): Any = {
        val si = ScalaVariableInfo(isAccessible = true, isLazy = false, obj, null, id, referenceManager.getRef(obj, id))
        parseInfo(si, depth - 1)
      }
    }
    profile {
      handlerManager.handleVariable(info, loopback, depth)
    }
  }

  private def filterVariableNames(interpreterVariablesNames: Seq[String]) = {
    val variablesNames = interpreterVariablesNames.seq
      .filter { name => !blackList.contains(name) }
      .filter { name => whiteList == null || whiteList.contains(name) }


    val p = Pattern.compile("res\\d*")
    val (resVariables, otherVariables: immutable.Seq[String]) = variablesNames.partition(x => p.matcher(x).matches())
    val sortedResVariables = resVariables
      .map(res => Try(res.stripPrefix("res").toInt))
      .filter(_.isSuccess)
      .map(_.get)
      .sortWith(_ > _)
      .take(interpreterResCountLimit)
      .map(num => "res" + num)

    val finalNames = otherVariables ++ sortedResVariables
    finalNames
  }

  //noinspection ScalaUnusedSymbol
  private implicit def toJavaFunction[A, B](f: A => B): JFunction[A, B] = new JFunction[A, B] {
    override def apply(a: A): B = f(a)
  }


  @inline
  def profile(body: => Any): Any = {
    if (enableProfiling) {
      val t = System.nanoTime()
      val newBody = body
      newBody match {
        case map: mutable.Map[String, Any] => map += (ResNames.TIME -> (System.currentTimeMillis() - t))
        case _ =>
      }
      newBody
    } else
      body
  }

  private def checkTimeout(startTimeout: Long, passed: Int, total: Int): Boolean = {
    val isTimeoutExceed = System.currentTimeMillis() - startTimeout > timeout
    if (isTimeoutExceed)
      errors += s"Variables collect timeout. Exceed ${timeout}ms. Parsed $passed from $total."
    isTimeoutExceed
  }
}