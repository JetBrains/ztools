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

import scala.collection.mutable
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{ILoop, IMain, JPrintWriter}

class ReplAware {

  trait Repl {
    def eval(code: String): Unit

    def getVariablesView(depth: Int = 3, enableProfiling: Boolean = false): VariablesView
  }

  def withRepl[T](body: Repl => T): T = {
    val classLoader = Thread.currentThread().getContextClassLoader
    beforeRepl()

    val iLoop = new ILoop(None, new JPrintWriter(Console.out, true))
    val settings = new Settings()
    settings.processArguments(List("-Yrepl-class-based"), processAll = true)
    settings.usejavacp.value = true
    iLoop.settings = settings
    iLoop.intp = new IMain(iLoop.settings)
    iLoop.initializeSynchronous()

    bindings(iLoop.intp)

    val wrapper = new ZtoolsInterpreterWrapper(iLoop.intp)

    def env(depth: Int, isProfilingEnabled: Boolean): VariablesViewImpl = new VariablesViewImpl(
      collectionSizeLimit = 100,
      stringSizeLimit = 400,
      blackList = "$intp,sc,spark,sqlContext,z,engine".split(",").toList,
      filterUnitResults = true,
      enableProfiling = isProfilingEnabled,
      depth = depth,
      timeout = 5000) {

      override def variables(): List[String] = iLoop.intp.definedSymbolList.filter { x => x.isGetter }.map(_.name.toString).distinct

      override def valueOfTerm(id: String): Option[Any] = wrapper.valueOfTerm(id)

      override def typeOfExpression(id: String): String = {
        val tpe = iLoop.intp.typeOfExpression(id, silent = true).toString()
        tpe
      }
    }

    val result = body(new Repl {
      override def eval(code: String): Unit = {
        iLoop.intp.interpret(code)
      }

      override def getVariablesView(depth: Int, enableProfiling: Boolean) = configure(env(depth, enableProfiling))
    })

    iLoop.closeInterpreter()
    afterRepl()
    Thread.currentThread().setContextClassLoader(classLoader)
    result
  }

  protected def configure(variablesView: VariablesViewImpl): VariablesView = variablesView

  protected def beforeRepl(): Unit = {}

  protected def afterRepl(): Unit = {}

  protected def bindings(intp: IMain): Unit = {}

  protected def getInPath[T](json: mutable.Map[String, Any], path: String): T = {
    val x :: xs = path.split('.').reverse.toList
    val data = xs.reverse.foldLeft(json) { (obj, key) =>
      obj(key).asInstanceOf[mutable.Map[String, Any]]
    }
    data(x).asInstanceOf[T]
  }
}
