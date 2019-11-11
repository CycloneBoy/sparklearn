// 第四章 集合

import scala.collection.mutable

var mutableSet = mutable.Set(1, 2, 3)

var immutableSet = Set(1, 3, 5, 6)


val numberArray = new Array[Int](10)

val strArray = new Array[String](10)

strArray(0) = "First element"

strArray

var strArray2 = Array("first", "second")


import scala.collection.mutable.ArrayBuffer

val strArrayVar = ArrayBuffer[String]()

strArrayVar += "hello"

strArrayVar += ("world", "programmer")

strArrayVar

strArrayVar ++= Array("wellcom", "to", "scala world")

strArrayVar ++= List("wellcom", "to", "scala world")

strArrayVar.trimEnd(3)

strArrayVar




var intArrayVar = ArrayBuffer(1, 1, 2)

intArrayVar.insert(0, 6)

intArrayVar

intArrayVar.insert(0, 7, 8, 9)

intArrayVar

intArrayVar.remove(0, 4)

intArrayVar.toArray

numberArray.toBuffer


for (i <- 0 to intArrayVar.length - 1) println("array element: " + intArrayVar(i))


for (i <- intArrayVar.indices) println("array element: " + intArrayVar(i))


for (i <- 0 until(intArrayVar.length, 2)) {
  println("array element :" + i)
}



for (i <- intArrayVar) println("array element " + i)

var intArrayNoBuffer = Array(1, 2, 3)

var intArrayNoBuffer2 = for (i <- intArrayNoBuffer) yield i * 2

intArrayNoBuffer2.sum
intArrayNoBuffer2.max
intArrayNoBuffer2.min
intArrayNoBuffer2.toString
intArrayNoBuffer2.mkString(",")


var multiDimArr = Array(Array(1, 2, 3), Array(2, 3, 4))
multiDimArr(0)(2)

for (i <- multiDimArr) println(i.mkString(","))

for (i <- multiDimArr) for (j <- i) print(j + " ")

// 列表
var nums = 1 :: (2 :: (3 :: (4 :: Nil)))
val nums2 = 1 :: 2 :: 3 :: 4 :: Nil

nums.isEmpty
nums.head
nums.tail

nums.tail.head

List(1, 2, 3) ::: List(4, 5, 6)

nums.init
nums.last
nums.reverse

nums.reverse.reverse == nums
nums.tail.reverse
nums.reverse.init

nums drop 3

nums take 1
nums.take(3)

nums.splitAt(2)

val chars = List('1', '2', '3', '4')
nums zip chars

nums.toString

nums.mkString
nums.toArray


List.apply(1, 2, 3)

List.range(2, 6)

List.range(6, 2, -1)


List.concat(List('a', 'b'), List('c'))

// 集合
val numsSet = Set(3.0, 5)
numsSet + 6

for (i <- numsSet) println(i)


// map
val studentInfo = Map("Jone" -> 21, "stephen" -> 22, "lucy" -> 33)
studentInfo.foreach(e => println(e._1 + ":" + e._2))

for (i <- studentInfo) println(i)

studentInfo.contains("Jone")


studentInfo.get("lucy")

studentInfo.get("bob")

// 队列
var intQueue = scala.collection.immutable.Queue(1, 2, 3)

intQueue.dequeue

intQueue.enqueue(4)

var intMutableQueue = mutable.Queue(2, 3, 5)

intMutableQueue += 5

intMutableQueue ++= List(4, 5, 6)


// 栈
val stack = new mutable.Stack[Int]()


