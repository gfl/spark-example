package org.example.antibiotics.operations

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.example.antibiotics.{AntibioticRecord, PrescriptionRecord}
import scala.reflect.ClassTag

/**
 * Common operations over the RDDs
 */
object CommonOperations {

  implicit class ConstructorRDD[T](rdd: RDD[T])(implicit ev1: ClassTag[String]) {

    /**
     * Transforms a String RDD into an antibiotic RDD
     * @return RDD with AntibioticRecords
     */
    def createAntibioticsRDD(implicit ev1: ClassTag[String]): RDD[AntibioticRecord] = rdd match {
      case stringRDD: RDD[String] => {
        stringRDD.asInstanceOf[RDD[String]].map(_.split(",")).map(a => AntibioticRecord(a))
      }
      case _ => throw new UnsupportedOperationException("method only applicable to RDD[String].")
    }

    /**
     * Transforms a String RDD into a prescription RDD
     * @return RDD with PrescriptionRecords
     */
    def createPrescriptionRDD: RDD[PrescriptionRecord] = rdd match {
      case stringRDD: RDD[String] => {
        stringRDD.asInstanceOf[RDD[String]].map(_.split(",")).map(b => PrescriptionRecord(b))
      }
      case _ => throw new UnsupportedOperationException("method only applicable to RDD[String].")
    }

  }

  implicit class PrescriptionsDF(prescriptions: DataFrame) {

    /**
     * Groups the antibiotic prescriptions by type, GP and period
     * @param antibiotics antibiotic RDD
     * @return
     */
    def aggregateAntibioticsByGP(antibiotics: DataFrame) = {

      // Group by Antibiotic type, GP and period. Equivalent Hive query:
      // SELECT p.sha, p.pct, p.practice, a.bnf_code_prefix, a.bnf_chemical_name, SUM(p.items) as total_items, SUM(p.act_cost) as total_cost, period
      // FROM gp_precriptions p JOIN antibiotics a ON p.bnf_code = a.bnf_code
      // GROUP BY p.sha, p.pct, p.practice, a.bnf_code_prefix, a.bnf_chemical_name, period;
      val result = prescriptions
        .join(antibiotics, prescriptions("bnfCode") === antibiotics("bnfCode"))
        .groupBy(prescriptions("sha"), prescriptions("pct"), prescriptions("practice"), antibiotics("bnfShortCode"), antibiotics("bnfChemicalName"), prescriptions("period"))
        .agg("items" -> "sum", "actCost" -> "sum")

      result
    }
  }

}
