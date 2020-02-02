package coop.rchain.rbalance.transitive
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

trait Closure[Key,Src] {  
  def computeClosure( src : Src, acc : Map[Key,Set[Src]], next : Src => Set[Src] ) : Map[Key,Set[Src]] = {
    val nextGen = next( src )
    nextGen.foldLeft( acc + ( key( src ) -> nextGen ) )( {
      ( a, x ) => {
        a.get( key( x ) ) match {
          case Some( v ) => a
          case None => computeClosure( x, a, next )
        }
      }
    } )
  }
  def next : Src => Set[Src]
  def key : Src => Key
  def close( src : Src ) : Map[Key,Set[Src]] = computeClosure( src, new HashMap[Key,Set[Src]](), next )
}

trait RHOCTxn {
  def src : String
  def trgt : String
  def amt : Float
  def hash : String
  def blockHash : String
  def justification : Set[RHOCTxn]
  override def toString() : String = {
    s"$src -$amt-> $trgt"
  }
}

class InitialRHOCTxn( override val trgt : String ) extends RHOCTxn {
  override def src : String = ""
  override def amt : Float = 0
  override def hash : String = ""
  override def blockHash : String = ""
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

  override def src           : String = txn.src
  override def trgt          : String = txn.trgt
  override def amt           : Float = txn.amt
  override def hash          : String = txn.hash
  override def blockHash     : String = txn.blockHash
  override def justification : Set[RHOCTxn] = txn.justification
}

class InitialAdjustment( val addr : String, override val taintedBalance : Float ) extends Adjustment {
  override def txn          = new InitialRHOCTxn( addr )
  override def cleanBalance = 0
}

case class ActualAdjustment( txn : RHOCTxn, cleanBalance : Float, taintedBalance : Float ) extends Adjustment

object RHOCTxnClosure extends Closure[String, RHOCTxn] {
  implicit val cs    : ContextShift[IO] = IO.contextShift(global)
  implicit val timer : Timer[IO]        = IO.timer(global)

  BlazeClientBuilder[IO](global).resource.use { client =>
    // use `client` here and return an `IO`.
    // the client will be acquired and shut down
    // automatically each time the `IO` is run.
    IO.unit
  }

  val blockingEC                        = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(5))
  val httpClient     : Client[IO]       = JavaNetClientBuilder[IO](blockingEC).create
  val apiKey         : String           = "251USXDI6XCV4CQYA6UCQ6Y5JPBR7FPXAC"
  val minBlockHeight : Int              = 7598478
  val maxBlockHeight : Int              = 9371743
  val barcelonaAddr  : String           = "0xEb148735F7e75B41AAF344CDa706b8F95d5E39d4"
  val feedback       : Int              = 1

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

  def getBalance( addr : String ) : Float = {
    throw new Exception( "not implemented yet" )
  }
  def getTaint( addr : String ) : Float = {
    throw new Exception( "not implemented yet" )
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

  def nextTxns( x : RHOCTxn ) : Set[RHOCTxn] = {
    x match {
      case adjustment : Adjustment => nextTxnAdjustments( adjustment )
      case rhocTxn    : RHOCTxn    => nextRHOCTxns( rhocTxn )      
    }
  }

  override def next = nextTxns
  override def key = _.trgt

  def closeAddr( addr : String ) = close( new InitialRHOCTxn( addr ) )
}

object GraphClosure extends Closure[String,( String, String )] {
  val g1 : Set[( String, String )] = {
    new HashSet[( String, String )]() + ( "a" -> "b" ) + ( "a" -> "c" ) + ( "b" -> "c" ) + ( "b" -> "d" ) + ( "c" -> "b" ) + ( "c" -> "d" ) + ( "d" -> "a" )
  }
  def nextEdges( g : Set[( String, String )] )( e : ( String, String ) ) = {
    val ( _, trgt ) = e
    g.filter(
      ( e1 : ( String, String ) ) => {
        val ( src, _ ) = e1
        ( src == trgt )
      }
    )
  }
  override def next = { ( e : ( String, String ) ) => nextEdges( g1 )( e ) }
  override def key = { 
    ( e : ( String, String ) ) => {
      val ( src, trgt ) = e
      trgt
    } 
  }
}




