package com.original.channel.model

/**
  * start up info
  */
class StartUpLogInfo(
  // Mandatory
  val startTime: Long, // epoch ms
  val endTime: Long // epoch ms
  // Optional
) extends Product with Serializable {

  override def productArity: Int = 2

  override def canEqual(that: Any): Boolean = that.isInstanceOf[StartUpLogInfo]

  override def productElement(idx: Int): Any = idx match {
    case 0 => startTime
    case 1 => endTime
  }
}
