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

}
