package org.example.antibiotics

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RDDImplicits {

  implicit class RichRDD[T: ClassTag](rdd: RDD[T]) {

    /**
     * Skips the input header of a CSV input file.
     * @return RDD
     */
    def skipHeader: RDD[T] = {
      rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    }

  }

}
