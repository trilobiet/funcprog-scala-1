package calculator

object Polynomial extends PolynomialInterface {

  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {

    Signal(
      b()*b() - 4*a()*c()
    )
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {

    // some intermediate signals
    val q = Signal( math.sqrt(delta()) )
    val a2 = Signal( 2*a() )

    Signal(
      if ( delta() < 0 ) Set()
      else {
        Set(
          ( -b() + q() ) / a2() ,
          ( -b() - q() ) / a2()
        )
      }
    )
  }

}
