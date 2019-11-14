import java.util.Comparator
// scala 与java 泛型互操作

case class Person(val name: String, val age: Int)

class PersonComparator extends Comparator[Person] {
  override def compare(o1: Person, o2: Person): Int = if (o1.age > o2.age) 1 else -1
}

val p1 = Person("bob", 20)
val p2 = Person("alis", 24)

val personComparator = new PersonComparator()
if (personComparator.compare(p1, p2) > 0) println(p1)
else println(p2)