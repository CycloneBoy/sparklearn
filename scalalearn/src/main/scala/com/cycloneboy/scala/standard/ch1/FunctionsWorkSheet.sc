// 值函数
var arrInt = Array(1, 2, 3, 4)
var increment = (x: Int) => x + 1
arrInt.map(increment)

arrInt.map((x: Int) => x + 1)

arrInt.map((x) => x + 1)
arrInt.map(x => x + 1)
arrInt.map(_ + 1)

var increment2 = (_: Int) + 1
var increment3 = 1 + (_: Int)
var increment4: Int => Int = 1 + _

def higherOrderFunction(f: Double => Double) = f(100)
def sqrt(x: Double) = Math.sqrt(x)
higherOrderFunction(sqrt)


def higherOrderFunction2(factor: Int) = (x: Double) => factor * x
val multiply = higherOrderFunction2(100)

multiply(10)

// 高阶函数
Array("spark", "scala", "hive").map(_ * 2)

val list = List("spark" -> 1, "hive" -> 2, "hadoop" -> 3)
list.map(x => x._1)
list.map(_._1)

val map = Map("spark" -> 1, "hive" -> 2, "hadoop" -> 3)
map.map(_._1)
map.map(_._2)

val listInt = List(1, 2, 3)

listInt.flatMap(x => x match {
  case 1 => List(1)
  case _ => List(x)
})

listInt.map(x => x match {
  case 1 => List(1)
  case _ => List(x * 2, x * 3, x * 4)
})

listInt.flatMap(x => x match {
  case 1 => List(1)
  case _ => List(x * 2, x * 3, x * 4)
})

var listInt2 = List(1, 4, 6, 8, 6, 7, 12, 9)
listInt2.filter(x => x > 6)
listInt2.filter(_ > 6)

listInt2.reduce(_ + _)
listInt2.reduce((x: Int, y: Int) => {
  println(x, y);
  x + y
})
listInt2.reduceLeft((x: Int, y: Int) => {
  println(x, y);
  x + y
})
listInt2.reduceRight((x: Int, y: Int) => {
  println(x, y);
  x + y
})

listInt2.fold(0)(_ + _)
listInt2.fold(100)(_ + _)

// 闭包

def a(f: Double => Double, p: Double => Unit) = {
  val x = f(10)
  p(x)
}

val f = (x: Double) => x * 2
val p = (x: Double) => println(x)
a(f, p)

val f2 = (x: Double) => x * x

a(f2, p)

// 函数柯里化
def multiply(factor: Int)(x: Double) = factor * x

multiply(10)(50)

val paf = multiply(10) _


paf(50)


def paf2 = multiply _

paf2(10)(50)

paf2(10)

def product(x1: Int, x2: Int, x3: Int) = x1 * x2 * x3

def product1 = product(_: Int, 2, 3)
product1(2)

def product2 = product(_: Int, _: Int, 3)
product2(2, 2)

def product3 = product(_: Int, _: Int, _: Int)
product3(2, 2, 3)

def product4 = product _
product4(2, 2, 3)




