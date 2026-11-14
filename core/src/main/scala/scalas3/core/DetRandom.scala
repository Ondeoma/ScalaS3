package scalas3.core

/** 決定的な乱数生成（SplitMix64）。状態を値として持ち回すので、Ref.modify の中でも決定的に使える。 */
final case class DetRandom(seed: Long):
  def nextRaw: (DetRandom, Long) =
    val s = seed + 0x9e3779b97f4a7c15L
    var z = s
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    (DetRandom(s), z ^ (z >>> 31))

  /** [0, bound) の一様乱数。bound <= 0 なら 0。 */
  def nextLong(bound: Long): (DetRandom, Long) =
    if bound <= 0 then (this, 0L)
    else
      val (r, v) = nextRaw
      (r, java.lang.Long.remainderUnsigned(v, bound))

  def nextInt(bound: Int): (DetRandom, Int) =
    val (r, v) = nextLong(bound.toLong)
    (r, v.toInt)

  def nextBoolean(probability: Double): (DetRandom, Boolean) =
    val (r, v) = nextLong(1L << 53)
    (r, v.toDouble / (1L << 53).toDouble < probability)
