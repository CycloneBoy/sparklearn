import scala.beans.BeanProperty
// 面向对象编程

class Person {
  @BeanProperty var name: String = null
}

val p = new Person

p.name = "Job"

p.getName
p.setName("alisi")


// 单例对象

object Student {

  private var studentNo: Int = 0

  def uniqueStudentNo() = {
    studentNo += 1
    studentNo
  }

}

println(Student.uniqueStudentNo())


object Example extends App {

  println("hhelo")
}


class Book(var name: String, var price: Int = 10) {
  println("constructing Book.....")

  override def toString: String = name + ":" + price
}


var b = new Book("b", 11)
b.name = "b1"
b.price = 20

println(b.name + " - " + b.price)

val b2 = new Book("tt")
val b3 = new Book("hello")

val b4 = new Book("1", 22)

class Car(var name: String, var price: Int, var color: String = "red") {

  def this(name: String) {
    this(name, 0, "")
    this.name = name
  }

  def this(name: String, price: Int) {
    this(name, price, "")
  }

  override def toString: String = name + ":" + price + ":" + color

}

val c1 = new Car("ben", 1)

var c2 = new Car("car")


class Annimo(var name: String, var age: Int) {
  println("constructing Annimo ....")

  override def toString: String = "name=" + name + ",age=" + age
}

class Cat(name: String, age: Int, var sex: Int) extends Annimo(name, age) {
  println("constructing Cat ....")

  override def toString: String = "name=" + name + ",age=" + age + ",sex=" + sex
}

println(new Annimo("cat", 20))
println(new Cat("cat1", 20, 0))


println(s"Person($c1,$c2)")



