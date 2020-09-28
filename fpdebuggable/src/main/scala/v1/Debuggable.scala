package v1

case class Debuggable (value: Int, message: String) {

  def map(f: Int => Int): Debuggable = {
    val newValue = f(value)
    Debuggable(newValue, message)
  }

  def flatMap(f: Int => Debuggable): Debuggable = {
    val newValue: Debuggable = f(value)
    Debuggable(newValue.value, message + "\n" + newValue.message)
  }

}

object Test extends App {

  val finalResult: Debuggable = for {
    fResult <- f(100)
    gResult <- g(fResult)
    hResult <- h(gResult)
  } yield hResult

  val p: Debuggable = f(100)

  // added a few "\n" to make the output easier
  // to read
  println(s"value:   ${finalResult.value}\n")
  println(s"message: \n${finalResult.message}")

  def f(a: Int): Debuggable = {
    val result = a * 2
    val message = s"f: a ($a) * 2 = $result."
    Debuggable(result, message)
  }

  def g(a: Int): Debuggable = {
    val result = a * 3
    val message = s"g: a ($a) * 3 = $result."
    Debuggable(result, message)
  }

  def g2(a: Debuggable): Debuggable = {
    val result = a.value * 3
    val message = s"g: a ($a) * 3 = $result."
    Debuggable(result, message)
  }

  def h(a: Int): Debuggable = {
    val result = a * 4
    val message = s"h: a ($a) * 4 = $result."
    Debuggable(result, message)
  }


  val o1 = Some("hello")
  val o2 = None
  val o3 = Some("world")
  val o4 = for {
    a <- o1
    b <- o2
    c <- o3
  } yield a + " " + c


}