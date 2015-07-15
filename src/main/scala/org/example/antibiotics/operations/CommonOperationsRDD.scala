package org.example.antibiotics.operations


import org.apache.spark.rdd.RDD
import org.example.antibiotics.{AntibioticRecord, PrescriptionRecord}

import scala.reflect.ClassTag

/**
 * Common operations over the RDDs
 */
object CommonOperationsRDD {

  implicit class ConstructorRDD[T](rdd: RDD[T])(implicit ev1: ClassTag[String]) {

    /**
     * Transforms a String RDD into an antibiotic RDD
     * @return RDD with AntibioticRecords
     */
    def createAntibioticsRDD(implicit ev1: ClassTag[String]): RDD[(String, AntibioticRecord)] = rdd match {
      case stringRDD: RDD[String] => {
        stringRDD.asInstanceOf[RDD[String]].map(_.split(",")).map(a => (a(0), AntibioticRecord(a)))
      }
      case _ => throw new UnsupportedOperationException("method only applicable to RDD[String].")
    }

    /**
     * Transforms a String RDD into a prescription RDD
     * @return RDD with PrescriptionRecords
     */
    def createPrescriptionRDD: RDD[(String, PrescriptionRecord)] = rdd match {
      case stringRDD: RDD[String] => {
        stringRDD.asInstanceOf[RDD[String]].map(_.split(",")).map(b => (b(3), PrescriptionRecord(b)))
      }
      case _ => throw new UnsupportedOperationException("method only applicable to RDD[String].")
    }

  }

  implicit class PrescriptionsRDD(rdd: RDD[(String, PrescriptionRecord)]) {

    /**
     * Groups the antibiotic prescriptions by type, GP and period
     * @param antibiotics antibiotic RDD
     * @return
     */
    def aggregateAntibioticsByGP(antibiotics: RDD[(String, AntibioticRecord)]) = {

      val prescriptionsWithAntibiotics: RDD[(String, (PrescriptionRecord, AntibioticRecord))] = rdd.join[AntibioticRecord](antibiotics)

      // Group by Antibiotic type, GP and period. Equivalent Hive query:
      // SELECT p.sha, p.pct, p.practice, a.bnf_code_prefix, a.bnf_chemical_name, SUM(p.items) as total_items, SUM(p.act_cost) as total_cost, period
      // FROM gp_prescriptions p JOIN antibiotics a ON p.bnf_code = a.bnf_code
      // GROUP BY p.sha, p.pct, p.practice, a.bnf_code_prefix, a.bnf_chemical_name, period;
      val result = prescriptionsWithAntibiotics.map(record => {
        val (prescription, antibiotic) = record._2
        ((prescription.sha, prescription.pct, prescription.practice, antibiotic.bnfShortCode, antibiotic.bnfChemicalName, prescription.period), (prescription.items, prescription.actCost))
      })
        .reduceByKey((values1, values2) => (values1._1 + values2._1, values1._2 + values2._2))

      result
    }
  }

}
