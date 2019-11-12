import java.util
// 类与类型

class Test

val a = new Test


classOf[Test]
a.getClass

var listStr = new util.ArrayList[String]()
var listInt = new util.ArrayList[Int]()

listStr.getClass
listInt.getClass

classOf[List[Int]]
classOf[List[String]]


var arrStr: Array[String] = Array("Hadoop", "Hive", "Spark")
var arrInt: Array[Int] = Array(1, 2, 3)

def printAll(x: Array[T] forSome {type T}) = {
  for (i <- x) {
    print(i + " ")
  }

  println()
}


printAll(arrStr)
printAll(arrInt)



def printAll2(x: Array[_]) = {
  for (i <- x) {
    print(i + " ")
  }

  println()
}

printAll2(arrStr)
printAll2(arrInt)

class TypeVariableBound {

  def compare[T <: Comparable[T]](first: T, second: T): T = {
    if (first.compareTo(second) > 0)
      first
    else
      second
  }
}

val tvb = new TypeVariableBound
println(tvb.compare("A", "B"))


case class Person(var name: String, var age: Int) extends Comparable[Person] {

  override def compareTo(o: Person): Int = {
    if (this.age > o.age) 1
    else if (this.age == o.age) 0
    else -1
  }

}

println(tvb.compare(Person("bob", 19), Person("johb", 22)))

// 类型界定  T <: R  限制T的最顶层类R,上界R
// 类型界定  T >: R  限制T必须是R的超类,下界R
// 视图定界 S <% Comparable[S]
// 类型变量界定要求类在类继承层次结构上
// 视图界定不但可以在类继承层次结构上,还可以跨越类继承层次结构
case class Student[T, S <% Comparable[S]](var name: T, var age: S)

val s1 = Student("john", "170")
val s2 = Student("bob", 170)

object Dog

import scala.reflect.runtime.universe.typeOf

typeOf[Dog.type]

//类型投影
class Outter {
  val x: Int = 0

  class Inner

  def test(i: Outter#Inner) = i

  //  def test(i:Inner)=i

}

val o1 = new Outter
val o2 = new Outter

val inner1 = new o1.Inner
val inner2 = new o2.Inner

o1.test(inner1)
o1.test(inner2)


val double2int = new Function1[Double, Int] {
  override def apply(v1: Double): Int = v1.toInt
}


// 抽象类型
abstract class Food

class Rice extends Food {
  override def toString: String = "粮食"
}

class Meat extends Food {
  override def toString: String = "肉"
}

class Animal {

  type FoodType

  def eat(f: FoodType) = f
}

class Human extends Animal {

  type FoodType = Food

  override def eat(f: FoodType): FoodType = f
}

class Tiger extends Animal {

  type FoodType = Meat

  override def eat(f: FoodType): FoodType = f
}

var human = new Human
var tiger = new Tiger

println("人可以吃: " + human.eat(new Rice))
println("人可以吃: " + human.eat(new Meat))
println("老虎可以吃: " + tiger.eat(new Meat))
//println("老虎可以吃: " + tiger.eat(new Rice))