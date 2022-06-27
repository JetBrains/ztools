import org.apache.commons.lang.exception.ExceptionUtils
import org.codehaus.jettison.json.JSONObject

try {
  import org.jetbrains.ztools.scala.{VariablesViewImpl, ZtoolsInterpreterWrapper}

  import scala.tools.nsc.interpreter.IMain


  val iMain: IMain = null
  val depth: Int = 0
  val filterUnitResults: Boolean = true
  val enableProfiling: Boolean = false
  val collectionSizeLimit = 100
  val stringSizeLimit = 400
  val blackList = "$intp,sc,spark,sqlContext,z,engine".split(',').toList
  val whiteList: Option[List[String]] = Option(null)


  def getVariables: String = {
    val iMainWrapper = new ZtoolsInterpreterWrapper(iMain)
    val variableView = new VariablesViewImpl(
      collectionSizeLimit = collectionSizeLimit,
      stringSizeLimit = stringSizeLimit,
      blackList = blackList,
      whiteList = whiteList.orNull,
      filterUnitResults = filterUnitResults,
      enableProfiling = enableProfiling,
      depth = depth) {

      override def variables() =
        iMain.definedSymbolList.filter { x => x.isGetter }.map(_.name.toString).distinct

      override def valueOfTerm(id: String): Option[Any] = iMainWrapper.valueOfTerm(id)

      override def typeOfExpression(id: String): String = iMain.typeOfExpression(id, silent = true).toString()
    }

    variableView.toFullJson.toString(2)
  }

  val variablesJson = getVariables
  println("--ztools-scala")
  println(variablesJson)
  println("--ztools-scala")
} catch {
  case t: Throwable =>
    val result = new JSONObject()
    val errors = Array[String](f"${ExceptionUtils.getMessage(t)}\n${ExceptionUtils.getStackTrace(t)}")
    result.put("errors", errors)
    result
}



