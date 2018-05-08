package svend.taxirides

import scala.util.Random

object Generators {

  type Gen[T] = Stream[T]

  /**
    * Creates a generator of Strings with sequencial integer values starting at 0, with a given prefix
    * */
  def sequencialGen(prefix: String): Gen[String] = {
    def rest(n: Int): Stream[String] = s"$prefix-$n" #:: rest(n+1)
    rest(0)
  }


  /**
    * Creates a generator of positive integer
    * */
  def randPosInt(maxValue: Int): Gen[Int] =  {
    val r = new Random()
    def rest: Stream[Int] = r.nextInt(maxValue) #:: rest
    rest
  }


}
