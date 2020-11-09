package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator extends CalculatorInterface {

  /**
   * a: 3
   * b: 5
   * c: a * 2
   * d: a + b
   * etc.
   * NB Only one operator per cell!
   *
   * @param namedExpressions
   */
  def computeValues(
      namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {

      for (
        (field, signal) <- namedExpressions
      ) yield {
        (field, Signal( eval( signal(), namedExpressions ) ))
      }

  }

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = {

    expr match {
      case Literal(x) => x
      case Ref(field) => eval( getReferenceExpr(field, references), references - field) // remove field from map
      case Plus(x,y) => eval(x, references) + eval(y, references)
      case Minus(x,y) => eval(x, references) - eval(y, references)
      case Times(x,y) => eval(x, references) * eval(y, references)
      case Divide(x,y) => eval(x, references) / eval(y, references)
      case _ => Double.NaN
    }

  }

  /** Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
      references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
