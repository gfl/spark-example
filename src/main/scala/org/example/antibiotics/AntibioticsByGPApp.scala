package org.example.antibiotics

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.example.antibiotics.RDDImplicits._
import org.example.antibiotics.operations.CommonOperationsRDD._


object AntibioticsByGPApp {
  def main (args: Array[String]) {
    // TODO Parametrize
    val prescriptionsPath = "/data/gp_prescriptions/*/*"
    val antibioticsPath = "/data/antibiotics/*"

    val conf = new SparkConf().setAppName("antibiotics-example")
    val sc = new SparkContext(conf)

    val antibiotics: RDD[(String, AntibioticRecord)] = sc.textFile(antibioticsPath).skipHeader.createAntibioticsRDD

    val prescriptions: RDD[(String, PrescriptionRecord)] = sc.textFile(prescriptionsPath).skipHeader.createPrescriptionRDD

  }
}
