package coop.rchain.rbalance.txns
import scala.collection.immutable.Set
import scala.collection.immutable.HashSet
import scala.collection.immutable.Map
import scala.collection.immutable.HashMap

//import scala.concurrent.ExecutionContext.global
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent._
import cats.effect._
import io.circe._
//import io.circe.literal._
import org.http4s._
import org.http4s.dsl.io._
import cats.effect._
import org.http4s.circe._
import org.http4s.implicits._
import org.http4s.client.blaze._
import org.http4s.client._
import coop.rchain.rbalance.transitive._

trait RHOCTxn extends Justified[RHOCTxn] {
  def src       : String
  def trgt      : String
  def amt       : Float
  def hash      : String
  def blockHash : String

  override def toString() : String = {
    s"$src -$amt-> $trgt"
  }
  
}

class InitialRHOCTxn( override val trgt : String ) extends RHOCTxn {
  override def src           : String       = ""
  override def amt           : Float        = 0
  override def hash          : String       = ""
  override def blockHash     : String       = ""
  override def justification : Set[RHOCTxn] = new HashSet[RHOCTxn]()
}
case class RHOCTxnRep( 
  src : String, trgt : String, 
  amt : Float, 
  hash : String, blockHash : String, 
  justification : Set[RHOCTxn] 
) extends RHOCTxn {
  override def equals( a : Any ) = {
    a match {
      case RHOCTxnRep( _, _, _, `hash`, _, _ ) => true
      case _ => false
    }
  }
  override def hashCode = ( hash ).##
}

trait Adjustment extends RHOCTxn {
  def txn                    : RHOCTxn
  def cleanBalance           : Float
  def taintedBalance         : Float

  override def src           : String       = txn.src
  override def trgt          : String       = txn.trgt
  override def amt           : Float        = txn.amt
  override def hash          : String       = txn.hash
  override def blockHash     : String       = txn.blockHash
  override def justification                = txn.justification
}

class InitialAdjustment( val addr : String, override val taintedBalance : Float ) extends Adjustment {
  override def txn          = new InitialRHOCTxn( addr )
  override def cleanBalance = 0
}

case class ActualAdjustment( txn : RHOCTxn, cleanBalance : Float, taintedBalance : Float ) extends Adjustment {  
}

object RHOCTxnClosure extends JustifiedClosure[String, RHOCTxn] 
    with InputCSVData {
  implicit val cs    : ContextShift[IO] = IO.contextShift(global)
  implicit val timer : Timer[IO]        = IO.timer(global)

  BlazeClientBuilder[IO](global).resource.use { client =>
    // use `client` here and return an `IO`.
    // the client will be acquired and shut down
    // automatically each time the `IO` is run.
    IO.unit
  }

  val blockingEC                          = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(5))
  val httpClient       : Client[IO]       = JavaNetClientBuilder[IO](blockingEC).create
  val apiKey           : String           = "251USXDI6XCV4CQYA6UCQ6Y5JPBR7FPXAC"
  val apiKey2          : String           = "AHMYX9PI91G4Q6PT772QY31M3668HJTNDR"
  val rhocContractAddr : String           = "0x168296bb09e24a88805cb9c33356536b980d3fc5"
  val minBlockHeight   : Int              = 7598478
  val maxBlockHeight   : Int              = 9371743
  val barcelonaAddr    : String           = "0xEb148735F7e75B41AAF344CDa706b8F95d5E39d4"
  val barcelonaTaint   : Float            = 11000000
  val feedback         : Int              = 1

  def recordTxn( 
    txn       : RHOCTxn, 
    srcAddr   : String, 
    trgtAddr  : String,
    amt       : Float, 
    hash      : String, 
    blockHash : String
  ) : RHOCTxn = {
    txn match {
      case adj : Adjustment => {
        val clean = getBalance( txn.trgt )
        val taint = getTaint( txn.src )
        val newTxn = new RHOCTxnRep( srcAddr, trgtAddr, amt, hash, blockHash, new HashSet[RHOCTxn]() + txn )
        new ActualAdjustment( newTxn, clean, taint )
      }
      case irt : RHOCTxn => {
        new RHOCTxnRep( srcAddr, trgtAddr, amt, hash, blockHash, new HashSet[RHOCTxn]() + txn )
      }      
    }
  }

  def etherscanTxnArray( addr : String ) : Vector[Json] = {
    val lowerCaseAddr = addr.toLowerCase()
    val etherscanURI =
      s"http://api.etherscan.io/api?module=account&action=tokentx&address=$lowerCaseAddr&startblock=$minBlockHeight&endblock=$maxBlockHeight&sort=asc&apikey=$apiKey"
    val etherscanDataStr = httpClient.expect[String]( etherscanURI ).unsafeRunSync
    val etherscanJson = Ok( etherscanDataStr ).flatMap( _.as[Json] ).unsafeRunSync
    val etherscanTxnRslt = etherscanJson \\ "result"
    etherscanTxnRslt( 0 ).asArray.getOrElse( throw new Exception( "not an array" ) )
  }

  def txnRecordData( addr : String, e : Json ) : Option[( String, String, Float, String, String )] = {
    val trgtAddr = ( e \\ "to" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
    val blockNumber = ( e \\ "blockNumber" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) ).toInt
    if ( ( trgtAddr != addr )  && ( blockNumber <= maxBlockHeight ) ){
      val srcAddr = ( e \\ "from" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
      val amt = ( e \\ "value" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) ).toFloat
      val hash = ( e \\ "hash" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
      val blockHash = ( e \\ "blockHash" )( 0 ).asString.getOrElse( throw new Exception( "not a string" ) )
      Some( ( srcAddr, trgtAddr, amt, hash, blockHash ) )
    }
    else {
      None
    }
  }

  def provideFeedback( txn : RHOCTxn, rslt : Set[RHOCTxn], lvl : Int ) : Unit = {
    feedback match {
      case lvl2 if lvl2 > 1 => {
        println( s"txn key: ${key(txn)}" )
        println( "Next generation:" )
        rslt.map( { txn => println( txn ) } )
      }
      case lvl1 if lvl1 > 0 => {
        val rsltSize = rslt.size
        println( s"txn key: ${key(txn)}" )
        println( s"Size of next generation:$rsltSize" )
      }
      case _ => {
      }
    }
  }

  def nextRHOCTxns( txn : RHOCTxn ) : Set[RHOCTxn] = {
    val lowerCaseAddr = txn.trgt.toLowerCase()
    val rslt = etherscanTxnArray( lowerCaseAddr ).foldLeft( new HashSet[RHOCTxn]() )(
      { ( acc, e ) => {
        txnRecordData( lowerCaseAddr, e ) match {
          case Some( ( srcAddr, trgtAddr, amt, hash, blockHash ) ) => {
            acc + recordTxn( txn, srcAddr, trgtAddr, amt, hash, blockHash )
          }
          case None => {
            acc
          }
        }
      } }
    )

    provideFeedback( txn, rslt, feedback )

    rslt
  }

  def loadAndFormatTxnData( source : String, dir : String ) : List[RHOCTxn] = {
    for( txnArray <- loadTxnData( source, dir ) ) yield {
      RHOCTxnRep(
        txnArray(4),
        txnArray(5),
        txnArray(6).toFloat,
        txnArray(0),
        txnArray(1),
        new HashSet[RHOCTxn]()
      )
    }
  }

  var txnDataV : Option[List[RHOCTxn]] = None
  def txnData() : List[RHOCTxn] = {
    txnDataV match {
      case None => {
        val txnD = loadAndFormatTxnData( txnSource, sourceDir )
        txnDataV = Some( txnD )
        txnD
      }
      case Some( txnD ) => txnD
    }
  }

  def nextRHOCTxnsD( txn : RHOCTxn ) : Set[RHOCTxn] = {
    txnData().filter( ( txnD ) => { txnD.src == txn.trgt } ).map(
      ( t ) => {
        RHOCTxnRep(
          t.src,
          t.trgt,
          t.amt,
          t.hash,
          t.blockHash,
          (new HashSet[RHOCTxn]() + txn)
        )
      }
    ).toSet
  }

  var addressListD : Option[List[String]] = None
  def addressList() : List[String] = {
    addressListD match {
      case None => {
        val addrS = 
          txnData().foldLeft( new HashSet[String]() )(
            ( acc, txn ) => { acc + txn.trgt }
          ).toList
        addressListD = Some( addrS )
        addrS
      }
      case Some( addrS ) => addrS
    }    
  }

  def getBalanceDataFromEtherscan( addr : String ) = {
    // Etherscan is not accepting this action!
    val etherscanURI =
      s"https://api.etherscan.io/api?module=account&action=tokenbalance&contractaddress=$rhocContractAddr=$addr&tag=latest&apikey=$apiKey"
    val etherscanDataStr = httpClient.expect[String]( etherscanURI ).unsafeRunSync
    val etherscanJson = Ok( etherscanDataStr ).flatMap( _.as[Json] ).unsafeRunSync
    val etherscanTxnRslt = etherscanJson \\ "result"
    //etherscanTxnRslt( 0 ).asArray.getOrElse( throw new Exception( "not an array" ) )
    etherscanTxnRslt
  }

  def loadAndFormatWalletData( source : String, dir : String ) : Map[String,Float] = {
    loadWalletData( source, dir ).foldLeft( new HashMap[String,Float]() )(
      ( acc, walletArray ) => {
        acc + ( walletArray( 0 ).toLowerCase -> walletArray( 1 ).toFloat )
      }
    )
  }

  var balancesD : Option[Map[String,Float]] = None
  def balances() : Map[String,Float] = {
    balancesD match {
      case None => {
        var balanceMap = loadAndFormatWalletData( walletSource, sourceDir )
        balancesD = Some( balanceMap )
        balanceMap
      }
      case Some( balanceMap ) => balanceMap
    }
  }

  def getBalance( addr : String ) : Float = {
    balances().get( addr ) match {
      case Some( balance ) => balance
        //case None => 0
      case None => -1
    }
  }
  def getTaint( addr : String ) : Float = {
    throw new Exception( "not implemented yet" )
  }
  def combine( adj : Adjustment, txn : RHOCTxn ) : ActualAdjustment = {
    ActualAdjustment( txn, getBalance( txn.trgt ), ( adj.txn.amt / getBalance( txn.src ) ) * adj.taintedBalance )
  }
  def combine( adj : Adjustment, adjNext : Adjustment ) : ActualAdjustment = {
    ActualAdjustment( adjNext.txn, getBalance( adj.txn.trgt ), ( adj.txn.amt / getBalance( adjNext.txn.src ) ) * adj.taintedBalance )
  }

  def nextTxnAdjustments( adjustment : Adjustment ) : Set[RHOCTxn] = {
    val txn = adjustment.txn
    val lowerCaseAddr = txn.trgt.toLowerCase()
    val rslt = etherscanTxnArray( lowerCaseAddr ).foldLeft( new HashSet[RHOCTxn]() )(
      { ( acc, e ) => {
        txnRecordData( lowerCaseAddr, e ) match {
          case Some( ( srcAddr, trgtAddr, amt, hash, blockHash ) ) => {
            acc + recordTxn( txn, srcAddr, trgtAddr, amt, hash, blockHash )
          }
          case None => {
            acc
          }
        }
      } }
    )

    provideFeedback( adjustment, rslt, feedback )

    rslt
  }

  def nextTxnAdjustmentsD( adj : Adjustment ) : Set[RHOCTxn] = {
    txnData().filter( ( txnD ) => { txnD.src == adj.trgt } ).map(
      ( t ) => {
        ActualAdjustment(
          RHOCTxnRep(
            t.src,
            t.trgt,
            t.amt,
            t.hash,
            t.blockHash,
            ( new HashSet[RHOCTxn]() + adj.txn )
          ),
          getBalance( t.trgt ),
          ( t.amt / getBalance( adj.src ) ) * adj.taintedBalance          
        )
      }
    ).toSet
  }

  def nextTxns( x : RHOCTxn ) : Set[RHOCTxn] = {
    x match {
      case adjustment : Adjustment => nextTxnAdjustmentsD( adjustment )
        //case rhocTxn    : RHOCTxn    => nextRHOCTxns( rhocTxn )
      case rhocTxn    : RHOCTxn    => nextRHOCTxnsD( rhocTxn )
    }
  }

  override def next = nextTxns
  override def key = _.trgt

  def closeAddr( addr : String ) = close( new InitialRHOCTxn( addr ) )
  def computeAdjustment( taint : Float )( path : List[_ <: RHOCTxn] ) : Adjustment = {
    path match {
      case Nil => new InitialAdjustment( "", 0 )
      case txn :: txns => {
        txns.foldLeft( ActualAdjustment( txn, getBalance( txn.src ), taint ) )(
          { ( acc, e ) => { combine( acc, e ) } }
        )
      }
    }
  }
  def computeAdjustments( addr : String, taint : Float ) : Map[String,Adjustment] = {
    computePaths( 
      new InitialAdjustment( addr, taint )
    ).foldLeft( new HashMap[String,Adjustment]() )(
      {
        ( acc, e ) => {
          val ( k, v ) = e
          val seed : Adjustment = new InitialAdjustment( "", 0 )
          val adj : Adjustment =            
            v.foldLeft( seed )(
              { 
                ( acc1, path ) => {
                  combine(
                    acc1, 
                    computeAdjustment( acc1.taintedBalance )( path.asInstanceOf[List[_ <: RHOCTxn]] )
                  )
                }
              }
            )
          acc + ( k -> adj )
        }
      }
    )
  }

}


