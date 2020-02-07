package coop.rchain.rbalance.txns
import scala.collection.immutable.HashMap

object AdjustmentConstants {
  val txnSource        : String             = "RHOC-tx_7598478-9371757.csv"
  val walletSource     : String             = "wallets_9371743_withzeros.txt"
  val sourceDir        : String             = "src/main/resources"

  val adjustmentsFile  : String             = "adjustments.csv"
  val proofFile        : String             = "proof.csv"
  val reportingDir     : String             = "src/main/resources" 

  val rhocContractAddr : String             = "0x168296bb09e24a88805cb9c33356536b980d3fc5"
  val minBlockHeight   : Int                = 7598478
  val maxBlockHeight   : Int                = 9371743
  val barcelonaLabel   : String             = "Barcelona"
  val barcelonaAddr    : String             = "0xEb148735F7e75B41AAF344CDa706b8F95d5E39d4"
  val barcelonaTaint   : Float              = 11000000
  val pithiaLabel      : String             = "Pithia"
  val pithiaAddr       : String             = "0xcd9910aea989e9b0b6b3e1192a474b5200e88c6b"
  val pithiaTaint      : Float              = 60000000
  val taintLabels      : Map[String,String] = {
    val m = new HashMap[String,String]()
    (m + ( pithiaLabel -> pithiaAddr ) + ( barcelonaLabel -> barcelonaAddr ))
  }
  val taintSources     : Map[String,Float]  = {
    val m = new HashMap[String,Float]()
    (m + ( barcelonaAddr -> barcelonaTaint ) + ( pithiaAddr -> pithiaTaint ))
  }
  val feedback         : Int               = 1
}

