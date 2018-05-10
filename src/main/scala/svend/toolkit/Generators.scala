package svend.toolkit

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

  def choose[A](values: Seq[A]) = {
    values(Random.nextInt(values.length))
  }


  def englishNameGen() = {

    // this is mostly a copy-paste form
    // https://github.com/halfninja/random-name-generator/blob/master/src/main/java/uk/co/halfninja/randomnames/EnglishlikeNameGenerator.java

    val givenNames = Seq(
      "Kevin","Dave","David","Alan","Derek","Paul","Nick","Mark","Sam","Dan","Robert","Gavin","Terry","Barry","Rahul","Steve","John","Naeem","Harris",
      "Natalie","Sarah","Deidre","Gladys","Penny","Rebecca","Grace","Kelly","Sally","Maggie","Kate","Kathryn")

    val lastNames = Seq("Jones","Smith","Baker","Havilland","Partridge","Rogers","Magnets","Trains","Rafferty","O'Toole","Hattes","Borde","Thorpe","Gravis","Twerp","Dingle","Schist","Granite","Evans","Gravel","Doon","Yaris","Corby","Toast","Greens","Havers");

    val firstName = choose(givenNames)
    val lastName = choose(lastNames)

    val r = Random.nextInt(a9)
    val suffix =
      if (r == 0) "ham"
      else if (r == 1) "son"
      else if (r == 2) "ton"
      else if (r ==3) "-" +choose(givenNames)
      else if (r ==4) "ford"
      else ""

    firstName + " " + lastName+suffix

  }


}
