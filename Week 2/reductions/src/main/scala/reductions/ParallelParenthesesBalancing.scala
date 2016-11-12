package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {
    def count(rest: Array[Char], acc: Int): Boolean = {
      if (rest.isEmpty) return acc == 0

      var state = 0
      if (rest.head == '(') state = 1
      if (rest.head == ')') {
        if (acc < 1) return false
        state = -1
      }

      count(rest.tail, acc + state)
    }
    count(chars, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, open: Int, unclosed: Int): (Int, Int) = {
      if (idx == until) return (open, unclosed)

      val (o, uc) = chars(idx) match {
        case '(' => (1, 0)
        case ')' => if (open > 0) (-1, 0) else (0, 1)
        case _ => (0, 0)
      }

      traverse(idx + 1, until, open + o, unclosed + uc)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      val mid = (until + from) / 2
      if (threshold >= (until - from)) {
        traverse(from, until, 0, 0)
      } else {
        val ((o1, uc1), (o2, uc2)) = parallel(reduce(from, mid), reduce(mid, until))
        (o1 - uc2 + o2, uc1)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
