import java.util.regex.Pattern
// 包类型

case class Dog(val name: String, val age: Int)

val dog = Dog("pet", 2)

def patternMatching(x: AnyRef) = x match {
  case Dog(_, age) => println(s"Dog name='', age=$age")
  case _ =>
}


patternMatching(dog)


val arrInt = Array(1, 2, 3, 4)

def patternMatching2(x: AnyRef) = x match {
  case Array(first, second) => println(s"序列中的第一个元素=$first,第二个=$second")
  case Array(first, _, three, _*) => println(s"序列中的第一个元素=$first,第三个=$three")
  case _ =>
}

patternMatching2(arrInt)


def patternMatching3(x: AnyRef) = x match {
  case d@Dog(_, _) => println(s"变量绑定模式返回的变量值:" + d)
  case _ =>
}

patternMatching3(dog)


class Cat(val name: String, val age: Int)

object Cat {

  def unapply(cat: Cat): Option[(String, Int)] = {
    if (cat != null) Some(cat.name, cat.age)
    else None
  }
}

val cat = new Cat("cat", 4)

def patternMatching4(x: AnyRef) = x match {
  case Cat(_, age) => println(s"Cat name='', age=$age")
  case _ =>
}

patternMatching4(cat)

// 正则表达式与模式匹配

val line = "Hadoop has been the most popular big data " +
  "processing tool since 2005-11-21"

var regx = "(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)"

val pattern = Pattern.compile(regx)

val m = pattern.matcher(line)

if (m.find()) {
  println(m.group(0))
  println(m.group(1))
  println(m.group(2))
  println(m.group(3))
} else {
  println("未找到匹配")
}

var rgex = """(\d\d\d\d)-(\d\d)-(\d\d)""".r


for (date <- rgex findAllIn "2005-11-21 2025-11-21") {
  println(date)
}

for (date <- rgex findAllMatchIn "2005-11-21 2025-11-21") {
  println(date)
  println(date.groupCount)
}

val copyright: String = rgex findFirstIn "date of this document: 2011-07-11" match {
  case Some(date) => "Copyright " + date
  case None => "no copyright"
}


var dateP2 = new scala.util.matching.Regex("""(\d\d\d\d)-(\d\d)-(\d\d)""", "year", "month", "day")

val copyright2: String = dateP2 findFirstMatchIn "2005-11-21 2025-11-21 2011-07-11" match {
  case Some(m) => "match " + m.group("year")
  case None => "no match"
}


val text = "From 2011-07-15 to 2011-11-11"

val rep1 = dateP2.replaceAllIn(text, m => m.group("month") + "/" + m.group("day"))

for ((language, "spark") <- Map("java" -> "hadoop", "scala" -> "spark", "closure" -> "storm")) {
  println(s"spark is developed by $language language")
}

for ((language, e@"spark") <- Map("java" -> "hadoop", "scala" -> "spark", "closure" -> "storm")) {
  println(s"$e is developed by $language language")
}

for ((language, framework: String) <- Map("java" -> "hadoop".length, "scala" -> "spark", "closure" -> "storm".length)) {
  println(s"$language is developed by $framework ")
}

for (Dog(name, age) <- List(Dog("pet", 2), Dog("pet5", 25), Dog("pet4", 24), Dog("pet2", 22))) {
  println(s"Dog $name is $age years old")
}


for (List(first, _*) <- List(List(1, 2, 5), List(6, 7, 8, 6, 4))) {
  println(s"First element is $first")
}


// 模式匹配与样例类.样例对象


sealed trait DeployMessage

case class RegisterWorker(id: String, host: String, port: Int) extends DeployMessage

case class UnRegisterWorker(id: String, host: String, port: Int) extends DeployMessage

case class Heartbeat(workerId: String) extends DeployMessage

case object RequestWorkerState extends DeployMessage

def handleMessage(msg: DeployMessage) = msg match {
  case RegisterWorker(id, host, port) => s"The worker $id is registering on $host:$port"
  case UnRegisterWorker(id, host, port) => s"The worker $id is unregistering on $host:$port"
  case Heartbeat(workerId) => s"The worker $workerId is sending heartbeat"
  case RequestWorkerState => "Request worker state"
}


val msgReg = RegisterWorker("1001", "199.163.0.1", 8080)
val msgUnReg = UnRegisterWorker("1002", "199.163.0.2", 8082)
val msgHB = Heartbeat("1003")
val msgWS = RequestWorkerState


println(handleMessage(msgReg))
println(handleMessage(msgUnReg))
println(handleMessage(msgHB))
println(handleMessage(msgWS))

/**
 *  1.在模式匹配应用时,case class 需要先创建对象,而case object 可以直接使用
 *  2.case class类会生成两个字节码文件,而case object中会生成一个字节码文件
 *  3.case class生成的伴生对象会自动实现apply及unapply方法,而case object中不会
 * 可以看到 使用case object 可以提升程序的执行效率,减少编译器的额外开销
 */