package org.example.antibiotics

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.example.antibiotics.RDDImplicits._
import org.example.antibiotics.operations.CommonOperations._


object AntibioticsByGPApp {

  def main (args: Array[String]) {
    // TODO Parametrize
    val prescriptionsPath = "/data/gp_prescriptions/*/*"
    val antibioticsPath = "/data/antibiotics/*"

    val conf = new SparkConf().setAppName("antibiotics-example")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val antibiotics = sc.textFile(antibioticsPath).skipHeader.createAntibioticsRDD.toDF

    val prescriptions = sc.textFile(prescriptionsPath).skipHeader.createPrescriptionRDD.toDF

    prescriptions.aggregateAntibioticsByGP(antibiotics).take(5).map(println)

    sc.stop()

  }
}
