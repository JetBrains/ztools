import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.spark.rdd.RDD
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import java.io.{PrintWriter, StringWriter}
import java.util


try {
  import scala.collection.mutable
  import scala.tools.nsc.interpreter.IMain

  trait Loopback {
    def pass(obj: Any, id: String): mutable.Map[String, Any]
  }
  object Names {
    val REF = "ref"
    val VALUE = "value"
    val TYPE = "type"
    val JVM_TYPE = "jvm-type"
    val LAZY = "lazy"
  }

  import java.util.function.Function

  object TrieMap {
    class Node[T](var value: Option[T]) {
      var children: mutable.Map[String, TrieMap.Node[T]] = _

      def put(key: String, node: TrieMap.Node[T]): Option[Node[T]] = {
        if (children == null)
          children = mutable.Map[String, TrieMap.Node[T]]()
        children.put(key, node)
      }

      def del(key: String): Option[Node[T]] = children.remove(key)

      def forEach(func: Function[T, _]): Unit = {
        func.apply(value.get)
        if (children != null) children.foreach(t => t._2.forEach(func))
      }
    }

    def split(key: String): Array[String] = {
      var n = 0
      var j = 0
      for (i <- 0 until key.length) {
        if (key.charAt(i) == '.') n += 1
      }
      val k = new Array[String](n + 1)
      val sb = new mutable.StringBuilder(k.length)
      for (i <- 0 until key.length) {
        val ch = key.charAt(i)
        if (ch == '.') {
          k({
            j += 1;
            j - 1
          }) = sb.toString
          sb.setLength(0)
        }
        else sb.append(ch)
      }
      k(j) = sb.toString
      k
    }
  }

  class TrieMap[T] {
    val root = new TrieMap.Node[T](null)

    def subtree(key: Array[String], length: Int): TrieMap.Node[T] = {
      var current = root
      var i = 0
      while ( {
        i < length && current != null
      }) {
        if (current.children == null) return null
        current = current.children.get(key(i)).orNull
        i += 1
      }
      current
    }

    def put(key: Array[String], value: T): Option[TrieMap.Node[T]] = {
      val node = subtree(key, key.length - 1)
      node.put(key(key.length - 1), new TrieMap.Node[T](Option.apply(value)))
    }

    def put(key: String, value: T): Option[TrieMap.Node[T]] = {
      val k = TrieMap.split(key)
      put(k, value)
    }

    def contains(key: String): Boolean = {
      val k = TrieMap.split(key)
      val node = subtree(k, k.length)
      node != null
    }

    def get(key: String): Option[T] = {
      val k = TrieMap.split(key)
      val node = subtree(k, k.length)
      if (node == null) return Option.empty
      node.value
    }

    def subtree(key: String): TrieMap.Node[T] = {
      val k = TrieMap.split(key)
      subtree(k, k.length)
    }
  }

  trait TypeHandler {
    def accept(obj: Any): Boolean

    def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any]
  }

  abstract class AbstractCollectionHandler(limit: Int) extends AbstractTypeHandler {
    trait Iterator {
      def hasNext: Boolean

      def next: Any
    }

    def iterator(obj: Any): Iterator

    def length(obj: Any): Int

    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
      json =>
        json += ("jvm-type" -> obj.getClass.getCanonicalName)
        json += ("length" -> length(obj))
        json += ("value" -> withJsonArray { json =>
          val it = iterator(obj)
          var index = 0
          while (it.hasNext && index < limit) {
            json += (extract(loopback.pass(it.next, s"$id[$index]")))
            index += 1
          }
        })
    }
  }

  abstract class AbstractTypeHandler extends TypeHandler {
    protected def withJsonArray(body: mutable.MutableList[Any] => Unit): mutable.MutableList[Any] = {
      val arr = mutable.MutableList[Any]()
      body(arr)
      arr
    }

    protected def withJsonObject(body: mutable.Map[String, Any] => Unit): mutable.Map[String, Any] = {
      val obj = mutable.Map[String, Any]()
      body(obj)
      obj
    }

    protected def extract(json: mutable.Map[String, Any]): Any =
      if (json.keys.size == 1)
        json(Names.VALUE)
      else
        json

    protected def wrap(obj: Any, tpe: String): mutable.Map[String, Any] = withJsonObject {
      json =>
        json += (Names.VALUE -> obj)
        json += (Names.TYPE -> tpe)
    }
  }

  class AnyValHandler extends AbstractTypeHandler {
    override def accept(obj: Any): Boolean =
      obj match {
        case _: Byte => true
        case _: Short => true
        case _: Boolean => true
        case _: Char => true
        case _: Int => true
        case _: Long => true
        case _: Float => true
        case _: Double => true
        case _ => false
      }

    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] =
      withJsonObject {
        json =>
          json += ("value" -> obj)
      }
  }

  class ArrayHandler(limit: Int) extends AbstractCollectionHandler(limit) {
    override def accept(obj: Any): Boolean = obj.isInstanceOf[Array[_]]

    override def length(obj: Any): Int = obj.asInstanceOf[Array[_]].length

    override def iterator(obj: Any): Iterator = new Iterator {
      private val it = obj.asInstanceOf[Array[_]].iterator

      override def hasNext: Boolean = it.hasNext

      override def next: Any = it.next
    }
  }

  class JavaCollectionHandler(limit: Int) extends AbstractCollectionHandler(limit) {
    override def accept(obj: Any): Boolean = obj.isInstanceOf[util.Collection[_]]

    override def iterator(obj: Any): Iterator = new Iterator() {
      private val it = obj.asInstanceOf[util.Collection[_]].iterator()

      override def hasNext: Boolean = it.hasNext

      override def next: Any = it.next()
    }

    override def length(obj: Any): Int = obj.asInstanceOf[util.Collection[_]].size()
  }

  class MapHandler(limit: Int) extends AbstractTypeHandler {
    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] =
      withJsonObject {
        json =>
          val map = obj.asInstanceOf[Map[_, _]]
          val keys = mutable.MutableList[Any]()
          val values = mutable.MutableList[Any]()
          json += ("jvm-type" -> obj.getClass.getCanonicalName)
          json += ("length" -> map.size)
          var index = 0
          map.view.take(math.min(limit, map.size)).foreach {
            case (key, value) =>
              keys += extract(loopback.pass(key, s"$id.key[$index]"))
              values += extract(loopback.pass(value, s"$id.value[$index]"))
          }
          index += 1
          json += ("key" -> keys)
          json += ("value" -> values)
      }

    override def accept(obj: Any): Boolean = obj.isInstanceOf[Map[_, _]]
  }

  class NullHandler extends AbstractTypeHandler {
    override def accept(obj: Any): Boolean = obj == null

    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = mutable.Map[String, Any]()
  }

  class SeqHandler(limit: Int) extends AbstractCollectionHandler(limit) {
    override def accept(obj: Any): Boolean = obj.isInstanceOf[Seq[_]]

    override def iterator(obj: Any): Iterator = new Iterator {
      private val it = obj.asInstanceOf[Seq[_]].iterator

      override def hasNext: Boolean = it.hasNext

      override def next: Any = it.next()
    }

    override def length(obj: Any): Int = obj.asInstanceOf[Seq[_]].size
  }

  class SetHandler(limit: Int) extends AbstractCollectionHandler(limit) {
    override def iterator(obj: Any): Iterator = new Iterator {
      private val it = obj.asInstanceOf[Set[_]].iterator

      override def hasNext: Boolean = it.hasNext

      override def next: Any = it.next()
    }

    override def length(obj: Any): Int = obj.asInstanceOf[Set[_]].size

    override def accept(obj: Any): Boolean = obj.isInstanceOf[Set[_]]
  }

  class SpecialsHandler(limit: Int) extends AbstractTypeHandler {
    override def accept(obj: Any): Boolean = obj.getClass.getCanonicalName != null && obj.getClass.getCanonicalName.startsWith("scala.")

    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
      json =>
        json += ("value" -> obj.toString.take(limit))
    }
  }

  class StringHandler(limit: Int) extends AbstractTypeHandler {
    override def accept(obj: Any): Boolean = obj.isInstanceOf[String]

    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
      json =>
        val str = obj.asInstanceOf[String]
        json += ("value" -> str.take(limit))
    }
  }

  class ThrowableHandler extends AbstractTypeHandler {
    override def accept(obj: Any): Boolean = obj.isInstanceOf[Throwable]

    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
      json =>
        val e = obj.asInstanceOf[Throwable]
        val writer = new StringWriter()
        val out = new PrintWriter(writer)
        e.printStackTrace(out)
        json += ("value" -> writer.toString)
    }
  }

  trait VariablesView {
    val errors: mutable.MutableList[String] = mutable.MutableList()

    def toJson: String

    def toJsonObject: mutable.Map[String, Any]

    def toJsonObject(path: String, deep: Int): mutable.Map[String, Any]

    def variables(): List[String]

    def valueOfTerm(id: String): Option[Any]

    def typeOfExpression(id: String): String
  }


  import org.apache.commons.lang.exception.ExceptionUtils
  import org.json4s.jackson.Serialization
  import org.json4s.{Formats, NoTypeHints}

  import java.util.function.{Function => JFunction}
  import scala.collection.mutable.ListBuffer
  import scala.language.implicitConversions

  abstract class VariablesViewImpl(val timeout: Int,
                                   val collectionSizeLimit: Int,
                                   val stringSizeLimit: Int,
                                   val blackList: List[String],
                                   val whiteList: List[String] = null,
                                   val filterUnitResults: Boolean,
                                   val enableProfiling: Boolean,
                                   val depth: Int) extends VariablesView {
    private val ru = scala.reflect.runtime.universe
    private val mirror = ru.runtimeMirror(getClass.getClassLoader)
    private val touched = mutable.Map[String, ScalaVariableInfo]()

    private val startTime = System.currentTimeMillis()

    class HandlerWrapper(val handler: TypeHandler) {
      def accept(info: ScalaVariableInfo): Boolean = info.isLazy || handler.accept(info.value)

      def handle(info: ScalaVariableInfo, loopback: Loopback): mutable.Map[String, Any] =
        if (info.isLazy) {
          val data = mutable.Map[String, Any]()
          data += (Names.LAZY -> true)
          data
        }
        else
          handler.handle(info.value, info.path, loopback)
    }

    private val handlerChain = ListBuffer[AbstractTypeHandler](
      new NullHandler(),
      new StringHandler(stringSizeLimit),
      new ArrayHandler(collectionSizeLimit),
      new JavaCollectionHandler(collectionSizeLimit),
      new SeqHandler(collectionSizeLimit),
      new SetHandler(collectionSizeLimit),
      new MapHandler(collectionSizeLimit),
      new ThrowableHandler(),
      new AnyValHandler(),
      new SpecialsHandler(stringSizeLimit),
      new DatasetHandler(),
      new RDDHandler(),
      new SparkContextHandler(),
      new SparkSessionHandler()
    ).map(new HandlerWrapper(_))

    val problems: mutable.Map[String, ReflectionProblem] = mutable.Map[String, ReflectionProblem]()

    private case class ScalaVariableInfo(isAccessible: Boolean,
                                         isLazy: Boolean,
                                         value: Any,
                                         tpe: String,
                                         path: String,
                                         ref: String) {
      val name: String = if (path != null) path.substring(path.lastIndexOf('.') + 1) else null
    }

    case class ReflectionProblem(e: Throwable, symbol: String, var count: Int)

    private class ReferenceWrapper(val ref: AnyRef) {
      override def hashCode(): Int = ref.hashCode()

      override def equals(obj: Any): Boolean = obj match {
        case value: ReferenceWrapper =>
          ref.eq(value.ref)
        case _ => false
      }
    }

    private val refMap = mutable.Map[ReferenceWrapper, String]()
    private val refInvMap = new TrieMap[ReferenceWrapper]()

    /**
     * Returns a reference (e.g. valid path) to the object or creates a record in reference maps (and returns null).
     *
     * @param obj  an object we want to find a reference for (can be null)
     * @param path path of the object e.g. myVar.myField.b
     * @return reference path to the object obj. The method returns null if obj is null itself or
     *         obj hasn't been mentioned earlier or in the case of AnyVal object.
     */
    private def getRef(obj: Any, path: String): String = {
      def clearRefIfPathExists(): Unit = {
        if (refInvMap.contains(path)) {
          val tree = refInvMap.subtree(path)
          tree.forEach(refMap.remove(_: ReferenceWrapper))
        }
      }

      obj match {
        case null | _: Unit =>
          clearRefIfPathExists()
          null
        case ref: AnyRef =>
          val wrapper = new ReferenceWrapper(ref)
          if (refMap.contains(wrapper)) {
            if (refInvMap.get(path).orNull != wrapper) clearRefIfPathExists()
            refMap(wrapper)
          } else {
            clearRefIfPathExists()
            refMap(wrapper) = path
            refInvMap.put(path, wrapper)
            null
          }
        case _ => null
      }
    }

    //noinspection ScalaUnusedSymbol
    private implicit def toJavaFunction[A, B](f: A => B): JFunction[A, B] = new JFunction[A, B] {
      override def apply(a: A): B = f(a)
    }

    private def getInfo(name: String): ScalaVariableInfo = {
      val obj = valueOfTerm(name).orNull
      ScalaVariableInfo(isAccessible = true, isLazy = false, obj, typeOfTerm(obj, name), name, null)
    }

    private def toJson(info: ScalaVariableInfo, depth: Int, path: String): mutable.Map[String, Any] = {
      object MyAnyHandler extends AbstractTypeHandler {
        override def accept(obj: Any): Boolean = true

        override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
          result =>
            if (depth > 0) withJsonObject {
              tree: mutable.Map[String, Any] =>
                if (info.value != null)
                  listAccessibleProperties(info).foreach { field =>
                    touched(field.path) = field
                    if (field.ref != null && field.ref != field.path) {
                      tree += (field.name -> (mutable.Map[String, Any]() += (Names.REF -> field.ref)))
                    } else {
                      tree += (field.name -> toJson(field, depth - 1, field.path))
                    }
                  }
                if (tree.nonEmpty)
                  result += (Names.VALUE -> tree)
                else
                  result += (Names.VALUE -> obj.toString.take(stringSizeLimit))
            } else {
              result += (Names.VALUE -> obj.toString.take(stringSizeLimit))
            }
            result += (Names.JVM_TYPE -> obj.getClass.getCanonicalName)
        }
      }
      val loopback = new Loopback {
        override def pass(obj: Any, id: String): mutable.Map[String, Any] = {
          val si = ScalaVariableInfo(isAccessible = true, isLazy = false, obj, null, id, getRef(obj, id))
          toJson(si, depth - 1, id)
        }
      }
      profile {
        val res = handlerChain.find(_.accept(info)) match {
          case Some(handler) => handler.handle(info, loopback)
          case _ => MyAnyHandler.handle(info.value, path, loopback)
        }
        if (info.tpe != null)
          res += (Names.TYPE -> info.tpe)
        res
      }
    }

    @inline
    def profile(body: => mutable.Map[String, Any]): mutable.Map[String, Any] = {
      if (enableProfiling) {
        val t = System.nanoTime()
        val newBody: mutable.Map[String, Any] = body
        newBody += ("time" -> (System.nanoTime() - t))
        newBody
      } else body
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
            return ScalaVariableInfo(isAccessible = tpe != "<notype>", isLazy = term.isLazy, value, tpe, fieldPath, getRef(value, fieldPath))
          }
        } catch {
          case e: Throwable => problems(path) = ReflectionProblem(e, symbol.toString, 1)
        }
      else
        problems(path).count += 1

      INACCESSIBLE
    }

    private val INACCESSIBLE = ScalaVariableInfo(isAccessible = false, isLazy = false, null, null, null, null)

    private def listAccessibleProperties(info: ScalaVariableInfo): List[ScalaVariableInfo] = {
      val instanceMirror = mirror.reflect(info.value)
      val instanceSymbol = instanceMirror.symbol
      val members = instanceSymbol.toType.members
      members.map {
        symbol => get(instanceMirror, symbol, info.path)
      }.filter(_.isAccessible).toList
    }

    def toFullJson: String = {
      val errors = problems.map { case (name, refProblem) =>
        f"$name: Reflection error for ${refProblem.symbol} counted ${refProblem.count} times.\n" +
          f"${ExceptionUtils.getMessage(refProblem.e)}\n${ExceptionUtils.getStackTrace(refProblem.e)}"
      }.toList

      implicit val ztoolsFormats: AnyRef with Formats = Serialization.formats(NoTypeHints)
      Serialization.write(
        Map(
          "variables" -> toJsonObject,
          "errors" -> (errors ++ this.errors.toList)
        )
      )
    }

    override def toJson: String = {
      implicit val ztoolsFormats: AnyRef with Formats = Serialization.formats(NoTypeHints)
      Serialization.write(toJsonObject)
    }

    override def toJsonObject: mutable.Map[String, Any] = {
      val result = mutable.Map[String, Any]()
      variables()
        .filter { name => !blackList.contains(name) }
        .filter { name => whiteList == null || whiteList.contains(name) }
        .foreach { name =>
          if (System.currentTimeMillis() - startTime > timeout) {
            errors += s"Variables collect timeout. Exceed ${timeout}ms."
            return result
          }
          try {
            val info = getInfo(name)
            val ref = getRef(info.value, name)
            if (!(filterUnitResults && isUnitResult(info))) {
              touched(info.path) = info
              if (ref != null && ref != info.path) {
                result += (info.path -> (mutable.Map[String, Any]() += (Names.REF -> ref)))
              } else {
                result += info.path -> toJson(info, depth, info.path)
              }
              result
            }
          } catch {
            case t: Throwable => errors +=
              f"${ExceptionUtils.getRootCauseMessage(t)}\n${ExceptionUtils.getStackTrace(ExceptionUtils.getRootCause(t))}"
          }
        }
      result
    }

    private def isUnitResult(info: ScalaVariableInfo): Boolean =
      info.name.length > 3 && info.name.startsWith("res") && info.name(3).isDigit && info.tpe == "Unit"

    override def toJsonObject(path: String, deep: Int): mutable.Map[String, Any] = {
      val result = mutable.Map[String, Any]()
      val obj = touched(path)
      if (obj.ref != null) {
        result += ("path" -> path)
        result += (Names.VALUE -> mutable.Map[String, Any]()) += (Names.REF -> obj.ref)
      } else {
        result += ("path" -> path)
        result += (Names.VALUE -> toJson(obj, depth, path))
      }
      result
    }

    def typeOfTerm(obj: Any, id: String): String = {
      obj match {
        case _: Boolean => "Boolean"
        case _: Byte => "Byte"
        case _: Char => "Char"
        case _: Short => "Short"
        case _: Int => "Int"
        case _: Long => "Long"
        case _: Float => "Float"
        case _: Double => "Double"
        case _: String => "String"
        case _: Unit => "Unit"
        case _ => typeOfExpression(id)
      }
    }
  }


  import scala.language.implicitConversions
  import scala.reflect.runtime.{universe => ru}

  //noinspection TypeAnnotation
  class ZtoolsInterpreterWrapper(val iMain: IMain) {

    import iMain.global._

    import scala.util.{Try => Trying}

    private lazy val importToGlobal = iMain.global mkImporter ru
    private lazy val importToRuntime = ru.internal createImporter iMain.global

    private implicit def importFromRu(sym: ru.Symbol) = importToGlobal importSymbol sym

    private implicit def importToRu(sym: Symbol): ru.Symbol = importToRuntime importSymbol sym

    // see https://github.com/scala/scala/pull/5852/commits/a9424205121f450dea2fe2aa281dd400a579a2b7
    def valueOfTerm(id: String): Option[Any] = exitingTyper {
      def fixClassBasedFullName(fullName: List[String]): List[String] = {
        if (settings.Yreplclassbased.value) {
          val line :: read :: rest = fullName
          line :: read :: "INSTANCE" :: rest
        } else fullName
      }

      def value(fullName: String) = {
        val universe = iMain.runtimeMirror.universe
        import universe.{InstanceMirror, Symbol, TermName}
        val pkg :: rest = fixClassBasedFullName((fullName split '.').toList)
        val top = iMain.runtimeMirror.staticPackage(pkg)

        @annotation.tailrec
        def loop(inst: InstanceMirror, cur: Symbol, path: List[String]): Option[Any] = {
          def mirrored =
            if (inst != null) inst
            else iMain.runtimeMirror reflect (iMain.runtimeMirror reflectModule cur.asModule).instance

          path match {
            case last :: Nil =>
              cur.typeSignature.decls find (x => x.name.toString == last && x.isAccessor) map { m =>
                (mirrored reflectMethod m.asMethod).apply()
              }
            case next :: rest =>
              val s = cur.typeSignature.member(TermName(next))
              val i =
                if (s.isModule) {
                  if (inst == null) null
                  else iMain.runtimeMirror reflect (inst reflectModule s.asModule).instance
                }
                else if (s.isAccessor) {
                  iMain.runtimeMirror reflect (mirrored reflectMethod s.asMethod).apply()
                }
                else {
                  assert(false, s.fullName)
                  inst
                }
              loop(i, s, rest)
            case Nil => None
          }
        }

        loop(null, top, rest)
      }

      Option(iMain.symbolOfTerm(id)) filter (_.exists) flatMap (s => Trying(value(s.fullName)).toOption.flatten)
    }
  }


  import org.apache.spark.sql.Dataset

  class DatasetHandler extends AbstractTypeHandler {
    override def accept(obj: Any): Boolean = obj.isInstanceOf[Dataset[_]]

    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
      json =>
        val df = obj.asInstanceOf[Dataset[_]]
        json += (Names.VALUE -> withJsonObject { json =>
          json += ("schema()" -> wrap(withJsonArray {
            schema =>
              df.schema.zipWithIndex.foreach {
                case (field, index) =>
                  schema += (extract(loopback.pass(field, s"$id.schema()[${index}]")))
              }
          }, "org.apache.spark.sql.types.StructType"))
          json += ("getStorageLevel()" -> wrap(df.storageLevel.toString(), "org.apache.spark.storage.StorageLevel"))
        })
    }
  }

  class RDDHandler extends AbstractTypeHandler {
    override def accept(obj: Any): Boolean = obj.isInstanceOf[RDD[_]]

    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
      json =>
        val rdd = obj.asInstanceOf[RDD[_]]
        json += (Names.VALUE -> withJsonObject { value =>
          value += ("getNumPartitions()" -> wrap(rdd.getNumPartitions, "Int"))
          value += ("name" -> wrap(rdd.name, "String"))
          value += ("id" -> wrap(rdd.id, "Int"))
          value += ("partitioner" -> wrap(rdd.partitioner.toString, "Option[org.apache.spark.Partitioner]"))
          value += ("getStorageLevel()" -> wrap(rdd.getStorageLevel.toString, "org.apache.spark.storage.StorageLevel"))
        })
    }
  }
  import org.apache.spark.SparkContext

  class SparkContextHandler extends AbstractTypeHandler {
    override def accept(obj: Any): Boolean = obj.isInstanceOf[SparkContext]

    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
      json =>
        val sc = obj.asInstanceOf[SparkContext]
        json += (Names.VALUE -> withJsonObject { json =>
          json += ("sparkUser" -> wrap(sc.sparkUser, "String"))
          json += ("sparkTime" -> wrap(sc.startTime, "Long"))
          json += ("applicationId()" -> wrap(sc.applicationId, "String"))
          json += ("applicationAttemptId()" -> wrap(sc.applicationAttemptId.toString, "Option[String]"))
          json += ("appName()" -> sc.appName)
        })
    }
  }


  import org.apache.spark.sql.SparkSession

  class SparkSessionHandler extends AbstractTypeHandler {
    override def accept(obj: Any): Boolean = obj.isInstanceOf[SparkSession]

    override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
      json =>
        val spark = obj.asInstanceOf[SparkSession]
        json += (Names.VALUE -> withJsonObject { json =>
          json += ("version()" -> spark.version)
          json += ("sparkContext" -> loopback.pass(spark.sparkContext, s"$id.sparkContext"))
          json += ("sharedState" -> loopback.pass(spark.sharedState, s"$id.sharedState"))
        })
    }
  }

  /**
   * Main section
   */
  val iMain: IMain = null
  val depth: Int = 0
  val filterUnitResults: Boolean = true
  val enableProfiling: Boolean = false
  val collectionSizeLimit = 100
  val stringSizeLimit = 400
  val timeout = 5000
  val blackList = "$intp,sc,spark,sqlContext,z,engine".split(',').toList
  val whiteList: Option[List[String]] = Option(null)


  def getVariables: String = {
    val iMainWrapper = new ZtoolsInterpreterWrapper(iMain)
    val variableView = new VariablesViewImpl(
      timeout = timeout,
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

    variableView.toFullJson
  }

  val variablesJson = getVariables
  println("--ztools-scala")
  println(variablesJson)
  println("--ztools-scala")
}
catch {
  case t: Throwable =>
    implicit val ztoolsFormats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val result = Map(
      "errors" -> Array(f"${ExceptionUtils.getMessage(t)}\n${ExceptionUtils.getStackTrace(t)}")
    )
    println("--ztools-scala")
    println(Serialization.write(result))
    println("--ztools-scala")
}



