package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000 //100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    // inner function
    def checkBalanced(chars: Array[Char], opened: Int): Boolean = {
      if (opened < 0) false
      else if (chars.isEmpty) opened == 0 // returns Boolean
      else if (chars.head == '(' ) checkBalanced( chars.tail, opened + 1 )
      else if (chars.head == ')' ) checkBalanced( chars.tail, opened - 1 )
      else checkBalanced( chars.tail, opened )
    }
    // start condition: there are no open brackets
    checkBalanced(chars,0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    println(chars.mkString(""))

    def traverse(idx: Int, until: Int, opened: Int, closed: Int): (Int,Int) = {
      if (idx == until) (opened, closed)
      else if (chars(idx) == '(') traverse(idx+1, until, opened+1, closed )
      // when encountering a closing bracket see if we can combine it with an open bracket
      else if (chars(idx) == ')') traverse(idx+1, until, if(opened>0) opened-1 else opened, if(opened>0) closed else closed+1)
      else traverse(idx+1, until, opened, closed)
    }

    def reduce(from: Int, until: Int): (Int,Int) = {
      if (until-from <= threshold)
        traverse(from, until,0 ,0)
      else {
        val mid = (until+from)/2
        val ( (leftOpened,leftClosed), (rightOpened,rightClosed) ) = parallel( reduce(from, mid), reduce(mid,until) )
        // uncomment this if you want to see what happens
        println( " ---> " + chars.slice(from,mid).mkString("")
          + " = " + (leftOpened,leftClosed) + "\n ---> "
          + chars.slice(mid,until).mkString("") + " = " + (rightOpened,rightClosed) )
        val inner = leftOpened - rightClosed
        (
          rightOpened + (if (inner > 0) inner else 0),
          leftClosed - (if (inner < 0) inner else 0)
        )
      }
    }

    // balanced if both opened and closed are zero
    reduce(0, chars.length) == (0,0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!
  /*

  */

}
