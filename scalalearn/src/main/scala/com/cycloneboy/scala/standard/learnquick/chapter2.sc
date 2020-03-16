var x: Int = 2
if (x > 0) 1 else -1

val s = if (x > 0) 1 else -1

val s1 = "hello"
var sum = 0
for (ch <- s1) {
  sum += ch
}
sum
println(sum)

for (i <- 1 to 10) yield i % 3


def fac(n: Int): Int = if (n <= 0) 1 else n * fac(n - 1)

fac(2)

def sum2(args: Int*) = {
  var result = 0
  for (elem <- args) result += elem
  result
}

println(sum2(4, 2, 3))
println(sum2(1 to 5: _*))

def box(s: String): Unit = {
  var border = "-" * s.length + "--\n"
  println(border + "|" + s + "|\n" + border)
}

box("test")

val nums = new Array[Int](10)

println(0 until 10)


val matrix = Array.ofDim[Double](3, 4)

matrix(2)(3) = 10











