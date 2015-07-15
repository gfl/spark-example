package org.example.antibiotics

/**
 * Created by GraziaFernandez on 27/03/2015.
 */
case class AntibioticRecord(bnfCode: String, bnfShortCode: String, bnfSectionName: String, bnfChemicalName: String,
                             drugName: String, preparationClass: Int, standardQuantityUnit: Int, itemsDispensed: Float, group: String)

object AntibioticRecord {
  def apply(elems: Array[String]) = new AntibioticRecord(elems(0).trim, elems(1).trim, elems(2).trim, elems(3).trim,
    elems(4).trim, elems(5).trim.toInt, elems(6).trim.toInt, elems(7).trim.toFloat, elems(8).trim)

}