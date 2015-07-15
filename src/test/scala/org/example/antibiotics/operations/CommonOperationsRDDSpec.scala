package org.example.antibiotics.operations

import org.example.antibiotics.{AntibioticRecord, PrescriptionRecord}
import org.example.testutils.SparkSpec
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.example.antibiotics.operations.CommonOperationsRDD._


class CommonOperationsRDDSpec extends FlatSpec with SparkSpec with GivenWhenThen with Matchers {

  behavior of "createAntibioticsRDD"

  it should "create the antibiotics RDD" in {

    Given("Antibiotics data")
    val input = Seq("BNF_CODE1,BNF_SHORT_CODE1,SECTION_NAME1,CHEMICAL_NAME1,DRUG_NAME1,3,1,15.0,GROUP1",
                   "BNF_CODE2,BNF_SHORT_CODE2,SECTION_NAME2,CHEMICAL_NAME2,DRUG_NAME2,4,2,12.0,GROUP2")

    When("creating the antibiotics RDD")
    val antibioticsRDD = sc.parallelize(input).createAntibioticsRDD

    Then("RDD with two records")
    val expectedRDD = sc.parallelize(Seq(("BNF_CODE1", new AntibioticRecord("BNF_CODE1","BNF_SHORT_CODE1","SECTION_NAME1","CHEMICAL_NAME1","DRUG_NAME1",3,1,15.0f,"GROUP1")),
      ("BNF_CODE2", new AntibioticRecord("BNF_CODE2","BNF_SHORT_CODE2","SECTION_NAME2","CHEMICAL_NAME2","DRUG_NAME2",4,2,12.0f,"GROUP2"))))
    antibioticsRDD shouldBe expectedRDD
  }

  behavior of "createPrescriptionRDD"

  it should "create the prescription RDD" in {

    Given("Prescriptions data")
    val input = Seq("SHA1,PCT1,PRACTICE1,BNF_CODE1,BNF NAME 1           ,0000001,00000000.63,00000000.61,0000050,201411,",
                    "SHA2,PCT2,PRACTICE2,BNF_CODE2,BNF NAME 2             ,0000002,00000001.13,00000001.15,0000021,201411,")

    When("creating the prescriptions RDD")
    val prescriptionsRDD = sc.parallelize(input).createPrescriptionRDD

    Then("RDD with two records")
    val expectedRDD = sc.parallelize(Seq(("BNF_CODE1", new PrescriptionRecord("SHA1","PCT1","PRACTICE1","BNF_CODE1","BNF NAME 1",1,0.63f,0.61f,50,"201411")),
      ("BNF_CODE2", new PrescriptionRecord("SHA2","PCT2","PRACTICE2","BNF_CODE2","BNF NAME 2",2,1.13f,1.5f,21,"201411"))))
    prescriptionsRDD shouldBe expectedRDD

  }

  behavior of "aggregateAntibioticsByGP"

  it should "ignore not antibiotics prescriptions" in {
    Given("Prescriptions that are not antibiotics")
    val prescriptions = Seq(("BN_CODEX", new PrescriptionRecord("SHA1","PCT1","PRACTICE1","BNF_CODEX","BNF_NAME1",1,0.63f,0.61f,50,"201411")))

    And("List of antibiotics")
    val antibiotics = Seq(("BNF_CODE1", new AntibioticRecord("BNF_CODE1","BNF_SHORT_CODE1","SECTION_NAME1","CHEMICAL_NAME1","DRUG_NAME1",3,1,15.0f,"GROUP1")),
      ("BNF_CODE2", new AntibioticRecord("BNF_CODE2","BNF_SHORT_CODE1","SECTION_NAME1","CHEMICAL_NAME1","DRUG_NAME2",3,1,12.0f,"GROUP1")),
      ("BNF_CODE3", new AntibioticRecord("BNF_CODE3","BNF_SHORT_CODE3","SECTION_NAME1","CHEMICAL_NAME3","Drug name 500mg Vl (Dry)",1,1,552.0f,"GROUP3")))

    When("aggregating prescriptions of antibiotics")
    val prescriptionsRDD1 = sc.parallelize(prescriptions)
    val aggPrescriptions = prescriptionsRDD1.aggregateAntibioticsByGP(sc.parallelize(antibiotics)).collect()

    Then("empty count")
    aggPrescriptions shouldBe empty
  }

  it should "count the antibiotics prescriptions" in {

    Given("Prescriptions that are antibiotics")
    val prescriptions2 = Seq(("BNF_CODE1", new PrescriptionRecord("SHA1","PCT1","PRACTICE1","BNF_CODE1","BNF_NAME1",1,0.63f,0.5f,50,"201411")),
      ("BNF_CODE2", new PrescriptionRecord("SHA2","PCT2","PRACTICE2","BNF_CODE2","BNF_NAME1",2,0.63f,1.5f,100,"201411")))

    And("List of antibiotics")
    val antibiotics = Seq(("BNF_CODE1", new AntibioticRecord("BNFCODE1","BNF_SHORT_CODE1","SECTION_NAME1","CHEMICAL_NAME1","DRUG_NAME1",3,1,15.0f,"GROUP1")),
      ("BNF_CODE2", new AntibioticRecord("BNF_CODE2","BNF_SHORT_CODE1","SECTION_NAME1","CHEMICAL_NAME1","DRUG_NAME2",3,1,12.0f,"GROUP1")),
      ("BNF_CODE3", new AntibioticRecord("BNF_CODE3","BNF_SHORT_CODE3","SECTION_NAME1","CHEMICAL_NAME3","Drug name 500mg Vl (Dry)",1,1,552.0f,"GROUP3")))

    When("aggregating prescriptions of antibiotics")
    val prescriptionsRDD2 = sc.parallelize(prescriptions2)
    val aggPrescriptions = prescriptionsRDD2.aggregateAntibioticsByGP(sc.parallelize(antibiotics)).collect()

    Then("empty count")
    // ((prescription.sha, prescription.pct, prescription.practice, antibiotic.bnfShortCode, antibiotic.bnfChemicalName, prescription.period), (prescription.items, prescription.actCost))
    val expectedOutput = Set(
      (("SHA1", "PCT1", "PRACTICE1", "BNF_SHORT_CODE1", "CHEMICAL_NAME1", "201411"), (1, 0.5f)),
      (("SHA2", "PCT2", "PRACTICE2", "BNF_SHORT_CODE1", "CHEMICAL_NAME1", "201411"), (2, 1.5f)))
    aggPrescriptions.toSet shouldBe expectedOutput

  }


}
