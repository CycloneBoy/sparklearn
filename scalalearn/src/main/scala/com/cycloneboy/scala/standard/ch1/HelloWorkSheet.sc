// 第二章 程序控制结构
println("hello")

var a = 10

for (i <- -1 to 5) {
  println(i)
}


import scala.util.control.Breaks._

breakable(

  for (i <- 1 to 5) {
    if (i > 2) break
    println(i)
  }
)


for (i <- 1 to 5 if (i < 3)) {
  println(i)
}


for (i <- 1 to 40 if (i % 4 == 0); if i % 5 == 0) {
  println(i)
}

var x = for (i <- 1 to 5) yield i % 2
