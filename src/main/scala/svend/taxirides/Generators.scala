package svend.taxirides

object Generators {

  /**
    * Creates a generators of Strings with sequencial integer values starting at 0, with a given prefix
    * */
  def sequencialGen(prefix: String): Stream[String] = {

    def rest(n: Int): Stream[String] = s"$prefix-$n" #:: rest(n+1)

    rest(0)
  }

}
