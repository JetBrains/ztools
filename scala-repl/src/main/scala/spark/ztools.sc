import org.jetbrains.ztools.scala.{VariablesViewImpl, ZtoolsInterpreterWrapper}

import scala.tools.nsc.interpreter.IMain


private val iMain: IMain = null
private val depth: Int = 0
private val filterUnitResults: Boolean = true
private val enableProfiling: Boolean = false
private val collectionSizeLimit = 100
private val stringSizeLimit = 400
private val blackList = "$intp,sc,spark,sqlContext,z,engine".split(',').toList


private def getVariables: String = {
  val iMainWrapper = new ZtoolsInterpreterWrapper(iMain)
  val variableView = new VariablesViewImpl(
    collectionSizeLimit = collectionSizeLimit,
    stringSizeLimit = stringSizeLimit,
    blackList = blackList,
    filterUnitResults = filterUnitResults,
    enableProfiling = enableProfiling,
    depth = depth) {

    override def variables() =
      iMain.definedSymbolList.filter { x => x.isGetter }.map(_.name.toString).distinct

    override def valueOfTerm(id: String): Option[Any] = iMainWrapper.valueOfTerm(id)

    override def typeOfExpression(id: String): String = iMain.typeOfExpression(id, silent = true).toString()
  }

  variableView.toJson
}

val variablesJson = getVariables
println("--ztools-scala")
println(variablesJson)
println("--ztools-scala")
