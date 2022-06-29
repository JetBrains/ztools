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

import org.junit.Assert.{assertEquals, assertNotNull, assertTrue}
import org.junit.Test

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.mutable

class VariablesViewImplTest extends ReplAware {
  val base = 0

  @Test
  def testSimpleVarsAndCollections(): Unit = {
    withRepl { intp =>
      intp.eval("val x = 1")
      val view = intp.getVariablesView()
      assertNotNull(view)
      var json = view.resolveVariables
      println(view.toJson)
      val x = json("x").asInstanceOf[Int]
      assertEquals(1, x)
      assertEquals(1, json.keys.size)

      intp.eval("val list = List(1,2,3,4)")
      json = view.resolveVariables
      println(view.toJson)
      val list = json("list").asInstanceOf[mutable.Map[String, Any]]
      assertEquals(3, list.keys.size)
      assertEquals(4, list("length"))
      assertEquals("List[Int]", list("type"))
      val values = list("value").asInstanceOf[mutable.MutableList[mutable.Map[String, Any]]]
      assertEquals((mutable.MutableList[Any]() += 1 += 2 += 3 += 4).toString, values.toString)
      assertEquals(2, json.keys.size)

      intp.eval("val map = Map(1 -> 2, 2 -> 3, 3 -> 4)")
      json = view.resolveVariables
      println(view.toJson)
      val map = json("map").asInstanceOf[mutable.Map[String, Any]]
      assertEquals(5, map.keys.size)
      assertEquals(3, map("length"))
      assertEquals("scala.collection.immutable.Map[Int,Int]", map("type"))
      val m = Map(1 -> 2, 2 -> 3, 3 -> 4)
      val key = map("key").asInstanceOf[mutable.MutableList[Any]].head.asInstanceOf[Int]
      val value = map("value").asInstanceOf[mutable.MutableList[Any]].head.asInstanceOf[Int]
      assertEquals(value, m(key))
      assertEquals(3, json.keys.size)

      intp.eval("1 + 1")
      json = view.resolveVariables
      val result = json(s"res$base").asInstanceOf[Int]
      assertEquals(2, result)
      assertEquals(4, json.keys.size)
    }
  }

  //noinspection AssertBetweenInconvertibleTypes
  @Test
  def testObjects(): Unit = {
    withRepl { intp =>
      intp.eval("class A(val x: Int)")
      intp.eval("val a = new A(1)")
      val view = intp.getVariablesView()
      assertNotNull(view)
      println(view.toJson)
      var json = view.resolveVariables
      val a = json("a").asInstanceOf[mutable.Map[String, Any]]
      assertEquals(2, a.keys.size)
      assertEquals("iw$A", a("type"))
      val aObj = a("value").asInstanceOf[mutable.Map[String, Any]]
      assertEquals(1, aObj.keys.size)
      val ax = aObj("x").asInstanceOf[Int]
      assertEquals(1, ax)
      // scala 2.11 returns scala.Int instead of Int in scala 2.12
      assertEquals(1, json.keys.size)

      val qDef = "class Q {\n" + "val a = Array(1,2,3)\n" + "val b = List(\"hello\", \"world\")\n" + "val c: List[List[String]] = List()\n" + "var y = 10\n" + "def m(): Int = 10\n" + "}"
      intp.eval(qDef)
      intp.eval("val q = new Q()")
      json = view.resolveVariables
      assertEquals("", view.errors.mkString(","))
      val qObj = json("q").asInstanceOf[mutable.Map[String, Any]]("value").asInstanceOf[mutable.Map[String, Any]]
      assertEquals(4, qObj.keys.size)
      assertEquals(3, qObj("a").asInstanceOf[mutable.Map[String, Any]]("length"))
      val tpe = qObj("c").asInstanceOf[mutable.Map[String, Any]]("type")
      assertTrue("scala.List[scala.List[String]]" == tpe || "List[List[String]]" == tpe)
    }
  }

  @Test
  def testReferences(): Unit = {
    withRepl { intp =>
      val view = intp.getVariablesView()
      assertNotNull(view)
      intp.eval("class A(var x: Int)")
      intp.eval("val a = new A(10)")
      intp.eval("class B(var q: A)")
      intp.eval("val b = new B(a)")
      intp.eval("val c = new B(a)")
      val json = view.resolveVariables
      assertEquals("", view.errors.mkString(","))
      assertEquals(3, json.keys.size) // a, b, c

      assertEquals("a", getInPath(json, "b.value.q.ref"))
      assertEquals("a", getInPath(json, "c.value.q.ref"))
    }
  }

  @Test
  def testBrokenReference(): Unit = {
    withRepl { intp =>
      val view = intp.getVariablesView()
      assertNotNull(view)
      intp.eval("class A(var x: Int)")
      intp.eval("val a = new A(10)")
      intp.eval("class B(var q: A)")
      intp.eval("val b = new B(a)")
      view.toJson
      intp.eval("val a = new A(11)") // top level term has been changed but looks the same

      val json = view.resolveVariables
      assertEquals(2, json.keys.size)
      assertEquals(10, getInPath[Int](json, "b.value.q.value.x"))
    }
  }

  @Test
  def testNull(): Unit = {
    withRepl { intp =>
      val view = intp.getVariablesView()
      assertNotNull(view)
      intp.eval("class A(var x: String)")
      intp.eval("val a = new A(null)")
      val json = view.resolveVariables
      println(view.toJson)
      assertEquals("String", getInPath(json, "a.value.x.type"))
      assertEquals(false, json("a").asInstanceOf[mutable.Map[String, Any]]("value").asInstanceOf[mutable.Map[String, Any]]("x").asInstanceOf[mutable.Map[String, Any]].keys.contains("value"))
    }
  }

  @Test
  def testReferenceInsideTheSameObject(): Unit = {
    withRepl { intp =>
      val view = intp.getVariablesView()
      assertNotNull(view)
      intp.eval("class A(var x: Int)")
      intp.eval("class B(var q: A, var p: A)")
      intp.eval("val b = new B(new A(10), null)")
      intp.eval("b.p = b.q")
      var json = view.resolveVariables
      assertEquals("b.p", getInPath(json, "b.value.q.ref"))

      intp.eval("b.q.x = 11")
      json = view.resolveVariables
      assertEquals(1, json.keys.size)
      assertEquals(11, getInPath[Int](json, "b.value.p.value.x"))
      assertEquals("b.p", getInPath(json, "b.value.q.ref"))

      intp.eval("b.p = null")
      json = view.resolveVariables
      assertEquals(1, json
      ("b").asInstanceOf[mutable.Map[String, Any]]
        ("value").asInstanceOf[mutable.Map[String, Any]]
        ("p").asInstanceOf[mutable.Map[String, Any]]
        .keys.size) // type only

      assertEquals(11, getInPath[Int](json, "b.value.q.value.x"))
    }
  }

  @Test
  def testTopLevelCyclicReferences(): Unit = {
    val code =
      """
        |class A {
        |  var strInA : String = _
        |  var memberB : B = _
        |}
        |
        |class B {
        |  var strInB : String = _
        |  var memberA : A = _
        |}
        |
        |val a = new A()
        |a.strInA = "class A"
        |
        |val b = new B()
        |b.strInB = "class B"
        |
        |a.memberB = b
        |b.memberA = a
        |""".stripMargin
    withRepl { intp =>
      val view = intp.getVariablesView()
      assertNotNull(view)
      intp.eval(code)
      val json = view.resolveVariables
      println(view.toJson)
      assertEquals("a", getInPath[String](json, "a.value.memberB.value.memberA.ref"))
      assertEquals("a.memberB", getInPath[String](json, "b.ref"))
      //      assertEquals("b", getInPath(json, "a.value.memberB.ref"))
      //      assertEquals("a", getInPath(json, "b.value.memberA.ref"))
      //      assertEquals("class A", getInPath(json, "a.value.strInA.value"))
      //      assertEquals("class B", getInPath(json, "b.value.strInB.value"))
    }
  }

  @Test
  def testCyclicReferences(): Unit = {
    val code =
      """
        |class A {
        |  var strInA : String = _
        |  var memberB : B = _
        |}
        |
        |class B {
        |  var strInB : String = _
        |  var memberA : A = _
        |}
        |
        |class C {
        |    val a = new A()
        |    val b = new B()
        |}
        |
        |val c = new C()
        |c.a.strInA = "class A"
        |c.b.strInB = "class B"
        |
        |c.a.memberB = c.b
        |c.b.memberA = c.a
        |""".stripMargin
    withRepl { intp =>
      val view = intp.getVariablesView()
      assertNotNull(view)
      intp.eval(code)
      val json = view.resolveVariables
      //      println(json.toString(2))
      assertEquals("c.b", getInPath(json, "c.value.a.value.memberB.ref"))
      assertEquals("c.a", getInPath(json, "c.value.b.value.memberA.ref"))
      assertEquals("class A", getInPath(json, "c.value.a.value.strInA.value"))
      assertEquals("class B", getInPath(json, "c.value.b.value.strInB.value"))
    }
  }

  //  @Test
  //  def testTraverseAlongThePath(): Unit = {
  //    val code =
  //      """
  //        |class A(val id: Int, val text: String)
  //        |class B(val a: A)
  //        |val a = new A(10, "Hello")
  //        |val b = new B(a)
  //        |""".stripMargin
  //    withRepl { intp =>
  //      intp.eval(code)
  //      val env = intp.getVariablesView
  //      val json = env.toJsonObject("b.a" ,2)
  //      println(json.toString(2))
  //      // {"path":"b.a","value":{"type":"A","value":{"id":{"type":"scala.Int","value":"10"}}}}
  //      assertEquals(2, json.keySet().size()) // path & value
  //      assertEquals("A", getInPath(json, "value.type"))
  //      assertEquals("10", getInPath(json, "value.value.id.value"))
  //    }
  //  }
  @Test
  def testTraverseAlongThePath() {
    val code =
      """
        |class A(val id: Int, val text: String)
        |class B(val a: A)
        |val a = new A(10, "Hello")
        |val b = new B(a)
        |""".stripMargin
    withRepl { repl =>
      repl.eval(code)
      val env = repl.getVariablesView(0)
      val j = env.resolveVariables
      println(env.toJson)
      assertTrue(getInPath[String](j, "a.value").startsWith("$line1.$"))
      //      println(env.toJsonObject.toString(2))
      val json = env.resolveVariable("b", 2)
      //      // {"path":"b.a","value":{"type":"Line_1.A","value":{"id":{"type":"kotlin.Int","value":"10"}}}}
      //      println(json.toString(2))
      assertEquals(1, json.keys.size) // path & value
      //      println(getInPath(json, "value.type"))
    }
  }

  //  @Test
  //  def testTraverseAlongThePathRefOrdering(): Unit = {
  //    val code =
  //      """
  //        |class A(val x: Int)
  //        |class B(val a: A)
  //        |class C(val b: B, val b1: B)
  //        |val a = new A(10)
  //        |val c = new C(new B(a), new B(a))
  //        |""".stripMargin
  //    withRepl { intp =>
  //      val view = intp.getVariablesView()
  //      assertNotNull(view)
  //      intp.eval(code)
  //      view.toJsonObject("c.b1.a", 2)
  //      val json = view.toJsonObject("c.b.a", 2)
  //      assertEquals("c.b1.a", getInPath(json, "value.ref"))
  //    }
  //  }

  @Test
  def testFunctions(): Unit = {
    withRepl { intp =>
      val view = intp.getVariablesView()
      assertNotNull(view)
      intp.eval("def method(): Int = 1")
      val json = view.resolveVariables
      // variables() must filter classes and methods
      // like that iLoop.intp.definedSymbolList.filter { x => x.isGetter }
      //      println(json.toString())
      assertTrue(json.isEmpty)
    }
  }

  @Test
  def testMethods(): Unit = {
    val code =
      """
        |class A {
        |val x = 10
        |def method(): Int = 1
        |}
        |val a = new A()
        |""".stripMargin
    withRepl { intp =>
      val view = intp.getVariablesView()
      assertNotNull(view)
      intp.eval(code)
      val json = view.resolveVariables
      assertEquals(1, json("a").asInstanceOf[mutable.Map[String, Any]]("value").asInstanceOf[mutable.Map[String, Any]].keys.size)
    }
  }

  @Test
  def testTypeChanged(): Unit = {
    withRepl { intp =>
      val view = intp.getVariablesView()
      intp.eval("val a = 2")
      view.resolveVariables
      intp.eval("val a = 2.0")
      val json = view.resolveVariables
      //      println(json.toString(2))
      assertEquals("2.0", getInPath[Double](json, "a").toString)
    }
  }

  @Test
  def testArray2D(): Unit = {
    withRepl { intp =>
      val view = intp.getVariablesView()
      intp.eval("val a: Array[Array[Int]] = Array(Array(1,2,3), Array(4,5,6))")
      var json = view.resolveVariables
      println(view.toJson)
      assertEquals("Array[Array[Int]]", getInPath(json, "a.type"))
      val arr = json("a").asInstanceOf[mutable.Map[String, Any]]("value").asInstanceOf[mutable.MutableList[Any]]
      assertEquals(2, arr.size)
      assertEquals(3, arr(0).asInstanceOf[mutable.Map[String, Any]]("length"))
      intp.eval("a(0)")
      json = view.resolveVariables
      assertEquals("a[0]", getInPath(json, "res0.ref"))
    }
  }

  @Test
  def testArrayOfObjects(): Unit = {
    withRepl { intp =>
      val view = intp.getVariablesView()
      intp.eval("class A(var x: Int)")
      intp.eval("val b = Array(new A(1), new A(2))")
      val json = view.resolveVariables
      println(view.toJson)
      assertEquals("Array[iw$A]", getInPath(json, "b.type"))
      val arr = json("b").asInstanceOf[mutable.Map[String, Any]]("value").asInstanceOf[mutable.MutableList[Any]]
      assertEquals(2, getInPath[Int](arr(1).asInstanceOf[mutable.Map[String, Any]], "value.x"))
    }
  }

  @Test
  def testLazy(): Unit =
    withRepl { intp =>
      val code =
        """
        class A {
          lazy val x: Int = throw new RuntimeException
        }
        val a = new A()
        """
      val view = intp.getVariablesView()
      intp.eval(code)
      val json = view.resolveVariables
      println(view.toJson)
      val obj = getInPath[mutable.Map[String, Any]](json, "a.value.x")
      assertEquals(2, obj.size)
      assertEquals("scala.Int", obj("type"))
      assertTrue(obj("lazy").asInstanceOf[Boolean])
    }

  @Test
  def testListOfAny(): Unit =
    withRepl { intp =>
      val view = intp.getVariablesView()
      val code =
        """
        class A(val x: Int)
        val a = List(1,Map(1->2),List(1,2,3), new A(10))
        val b = a(2) // inferred Any
        """
      intp.eval(code)
      val json = view.resolveVariables
      println(view.toJson)
      assertEquals("a[2]", getInPath[String](json, "b.ref"))
    }

  @Test
  def testBrokenRefInCollections(): Unit =
    withRepl { intp =>
      val view = intp.getVariablesView()
      val p1 =
        """
          |val a = List(Map("sd"->List(Set(0),2,3,"tttt")))
          |val b = a(0)("sd")
          |""".stripMargin
      intp.eval(p1)
      var json = view.resolveVariables
      assertEquals("a[0].value[0]", getInPath[String](json, "b.ref"))
      val p2 =
        """
          |val a = List(1,2,3)
          |""".stripMargin
      intp.eval(p2)
      json = view.resolveVariables
      assertEquals("", view.errors.mkString(","))
      // b isn't reference anymore
      assertEquals(4, getInPath[Int](json, "b.length"))
    }

  @Test
  def testBrokenRefInObject(): Unit =
    withRepl { intp =>
      val view = intp.getVariablesView()
      val code =
        """
      class C(z: Int)
      class B(val y: C)
      class A(var x: B)
      val a = new A(new B(new C(10)))
      val c = a.x.y
      """
      intp.eval(code)
      var json = view.resolveVariables
      //      println(json.toString(2))
      assertEquals("a.x.y", getInPath[String](json, "c.ref"))
      intp.eval("a.x = null")
      json = view.resolveVariables
      //      println(json.toString(2))
      assertEquals("iw$C", getInPath[String](json, "c.type"))
    }

  @Test
  def testBrokenRef(): Unit =
    withRepl { intp =>
      val view = intp.getVariablesView()
      val code1 =
        """
        import java.text.DateFormat
        import java.util.Date
        val a = List(Map("sd"->List(Set(0),2,3,DateFormat.getDateTimeInstance)))
        val c = Map("1"->Set(100))
        val d = c("1")

      """
      val code3 =
        """
        class A2(){
          val c = false
          val e = BigDecimal(2)
        }
        val t = new A2()
        val a = Map("5"->t)
        val b = a("5")
        val c = b
      """
      intp.eval(code1)
      var json = view.resolveVariables
      //      println(json.toString(2))
      println(">----------------")
      intp.eval(code3)
      json = view.resolveVariables
      //      println(json.toString(2))
      assertEquals("scala.collection.immutable.Set[Int]", getInPath[String](json, "d.type"))
    }

  //  @Test
  //  def testPerformance(): Unit = {
  //    withRepl { intp =>
  //      val view = intp.getVariablesView()
  //      Range(1, 100).foreach { _ =>
  //        intp.eval("val a = 2.0")
  //        val time = System.currentTimeMillis()
  //        val json = view.toJsonObject
  //        val t = System.currentTimeMillis() - time
  ////        println("time = " + t + " ms")
  //      }
  //      val json = view.toJsonObject
  //      println(json.toString(2))
  //    }
  //  }

  //    @Test
  //    def testPerformance2(): Unit = {
  //      withRepl { intp =>
  //        val view = intp.getVariablesView()
  //        intp.eval("val a = 2.0")
  //        Range(1, 200).foreach { _ =>
  //          intp.eval("println(\"hello\")")
  //          val time = System.currentTimeMillis()
  //          val json = view.toJsonObject
  //          val t = System.currentTimeMillis() - time
  //          println("time = " + t + " ms")
  //        }
  //        val json = view.toJsonObject
  //        println(json.toString(2))
  //      }
  //    }

  //  @Test
  //  def testPerformance3(): Unit = {
  //      withRepl { intp =>
  //        val view = intp.getVariablesView()
  //        var json: JSONObject = null
  //        Range(1, 200).foreach { _ =>
  //          intp.eval("val a = 2\nThread.sleep(5)")
  //          intp.eval("val b = 3\nThread.sleep(5)")
  //          val time = System.currentTimeMillis()
  //          json = view.toJsonObject
  //          val t = System.currentTimeMillis() - time
  //          println("time = " + t + " ms")
  //        }
  //        println(json.toString(2))
  //      }
  //  }
}
