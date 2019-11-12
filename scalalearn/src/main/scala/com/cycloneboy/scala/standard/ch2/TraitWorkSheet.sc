import java.io.PrintWriter
// trait 特征

trait Closable {
  def close(): Unit
}

class File(var name: String) extends Closable {
  override def close(): Unit = println(s"File $name has been closed")
}

new File("confit.txt").close()

abstract class absFile(name: String) extends File(name) with Cloneable;

trait Logger {
  println("Logger")

  def log(msg: String): Unit
}

trait FileLogger extends Logger {
  println("FileLogger")

  val fileName: String

  lazy val fileOutput = new PrintWriter(fileName: String)


  override def log(msg: String): Unit = {
    fileOutput.println(msg)
    fileOutput.flush()
  }

}

trait Closeable {
  println("Closeable")
}

class Person() {
  println("Constructing person...")
}

class Student extends Person with FileLogger {
  println("Constructing student...")

  val fileName = "file.log"


}


//new FileLogger {}.log("trait")

//new Student().log("trait demo")

val s = new Student
s.log("#")
s.log("#lazy demo")






