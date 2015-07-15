package org.example.antibiotics

/**
 * Created by GraziaFernandez on 27/03/2015.
 */
case class PrescriptionRecord(sha: String, pct: String, practice: String, bnfCode: String, bnfName: String, items: Int,
                              nic: Float, actCost: Float, quantity: Int, period: String)

object PrescriptionRecord {
  def apply(elems: Array[String]) =
    new PrescriptionRecord(elems(0).trim, elems(1).trim, elems(2).trim, elems(3).trim, elems(4).trim, elems(5).trim.toInt,
      elems(6).trim.toFloat, elems(7).trim.toFloat, elems(8).trim.toInt, elems(9).trim)
}