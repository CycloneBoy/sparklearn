
// 隐式转换

1 to 5

var x: Float = 1

implicit def float2int(x: Float) = x.toInt

var intV: Int = 1.3f

implicit class Dog(val name: String) {
  def bark = println(s"$name is barking")
}

"Nack".bark

trait Multplicalbe[T] {
  def multiply(x: T): T
}

implicit object MultplicalbeInt extends Multplicalbe[Int] {
  override def multiply(x: Int): Int = x * x
}

implicit object MultplicalbeString extends Multplicalbe[String] {
  override def multiply(x: String): String = x * 2
}

def multiply[T: Multplicalbe](x: T) = {
  // implicitly 方法,访问隐式对象
  val ev = implicitly[Multplicalbe[T]]

  // 根据具体的类型调用相应的隐式对象中的方法
  ev.multiply(x)
}

def multiply2[T: Multplicalbe](x: T)(implicit ev: Multplicalbe[T]) = {
  // implicitly 方法,访问隐式对象
  //  val ev = implicitly[Multplicalbe[T]]

  // 根据具体的类型调用相应的隐式对象中的方法
  ev.multiply(x)
}

println(multiply(5))
println(multiply("5"))

println(multiply2(5))
println(multiply2("5"))

// 隐式值
implicit val x: Double = 2.55

def sqrt(implicit x: Double) = Math.sqrt(x)

sqrt


def product(implicit x: Double, y: Double) = x * y

//implicit val d = 2.55

product

def product2(x: Double)(implicit y: Double) = x * y

product2(3)

