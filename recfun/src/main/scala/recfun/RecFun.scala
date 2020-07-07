package recfun

object RecFun extends RecFunInterface {

  def main(args: Array[String]): Unit = {
    /*
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(s"${pascal(col, row)} ")
      println()
    }
    val isBalanced = balance("".toList)
    println(isBalanced)
    */
    println(countChange(100, List(1,5,10,25,50) ) )
  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int = {
    if ( c==0 || c==r ) 1
    else pascal(c-1,r-1) + pascal(c,r-1)
  }


  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = {
    // inner function
    def checkBalanced(chars: List[Char], opened: Int): Boolean = {
      if (opened < 0) false
      else if (chars.isEmpty) opened == 0 // returns Boolean
      else if (chars.head == '(' ) checkBalanced( chars.tail, opened + 1 )
      else if (chars.head == ')' ) checkBalanced( chars.tail, opened - 1 )
      else checkBalanced( chars.tail, opened )
    }
    // start condition: there are no open brackets
    checkBalanced(chars,0)
  }

  /**
   * Exercise 3
   * See 1.2.2 Tree recursion Abelson/Sussman
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    if (money == 0) 1
    else if (money < 0 || coins.isEmpty) 0
    else countChange( money, coins.tail) + countChange( money - coins.head, coins )
  }
}
