package coop.rchain.rbalance.txns
import java.io._
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

trait EdgeT[Node] {
  def src       : Node
  def trgt      : Node
  def weight    : Float
  def hash      : String
  def blockId   : String

  override def toString() : String = {
    s"$src -$weight-> $trgt"
  }
}

case class Edge[Node](
  override val src           : Node,
  override val trgt          : Node,
  override val weight        : Float,
  override val hash          : String,
  override val blockId       : String,
  override val justification : Set[Edge[Node]]
) extends EdgeT[Node] with Justified[Edge[Node]]

trait AddressT {
  def addr      : String
  def balance   : Float
}

case class Address( 
  override val addr    : String, 
  override val balance : Float
) extends AddressT {
  override def equals( a : Any ) = {
    a match {
      case Address( `addr`, _ ) => true
      case _ => false
    }
  }
  override def hashCode = ( addr ).##
}

trait RHOCTxnEdge extends EdgeT[Address] with Justified[RHOCTxnEdge]

case class RHOCTxnIdentity(
//  override val src           : Address,
//  override val trgt          : Address,
  val addr                   : Address,
  override val weight        : Float,
  override val hash          : String,
  override val blockId       : String,
  override val justification : Set[RHOCTxnEdge]
) extends RHOCTxnEdge {
  override def src = addr
  override def trgt = addr
}

case class RHOCTxnEdgeRep(
  override val src           : Address,
  override val trgt          : Address,
  override val weight        : Float,
  override val hash          : String,
  override val blockId       : String,
  override val justification : Set[RHOCTxnEdge]
) extends RHOCTxnEdge

object RHOCTxnGraphClosure 
    extends JustifiedClosure[Address, RHOCTxnEdge] with InputCSVData {
  import AdjustmentConstants._

  // Use this initial edge to generate the Barcelona clique
  val barcelonaEdge : RHOCTxnEdge = {
    new RHOCTxnIdentity(
      new Address( barcelonaAddr.toLowerCase, barcelonaTaint ),
      barcelonaTaint,
      "scam",
      "scam",
      new HashSet[RHOCTxnEdge]()
    )
  }

  // Use this initial edge to generate the Pithia clique
  val pithiaEdge : RHOCTxnEdge = {
    new RHOCTxnIdentity(
      new Address( pithiaAddr.toLowerCase, pithiaTaint ),
      pithiaTaint,
      "scam",
      "scam",
      new HashSet[RHOCTxnEdge]()
    )
  }

  def loadAndFormatWalletData() : Map[String,Float] = {
    loadWalletData().foldLeft( new HashMap[String,Float]() )(
      ( acc, walletArray ) => {
        acc + ( walletArray( 0 ).toLowerCase -> walletArray( 1 ).toFloat )
      }
    )
  }

  var balancesD : Option[Map[String,Float]] = None
  def balances() : Map[String,Float] = {
    balancesD match {
      case None => {
        val balanceMap = loadAndFormatWalletData()
        balancesD = Some( balanceMap )
        balanceMap
      }
      case Some( balanceMap ) => balanceMap
    }
  }
    
  def loadAndFormatTxnData() : List[RHOCTxnEdge] = {
    for( txnArray <- loadTxnData() ) yield {
      RHOCTxnEdgeRep(
        new Address( txnArray(4), 0 ),
        new Address( txnArray(5), 0 ),
        txnArray(6).toFloat,
        txnArray(0),
        txnArray(1),
        new HashSet[RHOCTxnEdge]()
      )
    }
  }

  var txnDataV : Option[List[RHOCTxnEdge]] = None
  def txnData() : List[RHOCTxnEdge] = {
    txnDataV match {
      case None => {
        val txnD = loadAndFormatTxnData()
        txnDataV = Some( txnD )
        txnD
      }
      case Some( txnD ) => txnD
    }
  }

  // We run the transitive closure to generate the tainted
  // clique, i.e. the graph of transactions that communicate taint.
  // During the calculation of the transitive closure we calculate the
  // weight of each edge to be the fraction of the amt of the
  // transaction divided by the sum of all the outgoing transactions.

  def nextRHOCTxnWeightedEdges( txn : RHOCTxnEdge ) : Set[RHOCTxnEdge] = {
    txnData().filter( ( txnD ) => { txnD.src == txn.trgt } ) match {
      case Nil     => new HashSet[RHOCTxnEdge]()
      case progeny => {
        val seed : Float = 0
        val totalWeight = progeny.foldLeft( seed )( ( acc, t ) => { acc + t.weight } )
        progeny.map(
          ( t ) => {
            RHOCTxnEdgeRep(
              t.src,
              t.trgt,
              ( t.weight / totalWeight ),
              t.hash,
              t.blockId,
              (new HashSet[RHOCTxnEdge]() + txn)
            )
          }
        ).toSet
      }
    }        
  }    

  override def next = nextRHOCTxnWeightedEdges
  override def key = _.trgt

  def taintedClique( taintMap : Map[Address,Set[_ <: RHOCTxnEdge]] ) : List[RHOCTxnEdge] = {
    taintMap.values.foldLeft( new HashSet[RHOCTxnEdge]() )( ( acc, s ) => { acc ++ s } ).toList
  }

  def getClique( taintedEdge : RHOCTxnEdge ) : List[RHOCTxnEdge] = { taintedClique( close( taintedEdge ) ) }

  // Now, we can rerun the closure just on the clique and calculate
  // the taint at an address as the application of the weight to the
  // "balance" of the target address of the edge. Note that the balance here is
  // just the taint because we begin from the root of the clique where
  // the balance is entirely taint and carry that forward through the
  // calculation of the closure.

  def nextRHOCTxnTaint( clique : List[RHOCTxnEdge] )( txn : RHOCTxnEdge ) : Set[RHOCTxnEdge] = {
    val progeny : List[RHOCTxnEdge] = clique.filter( ( txnD ) => { txnD.src == txn.trgt } )
    progeny.map(
      ( t ) => {
        val trgtTaint : Float = txn.trgt.balance * t.weight
        //println( s"$t : $trgtTaint" )
        RHOCTxnEdgeRep(
          t.src,
          new Address( t.trgt.addr, trgtTaint ),
          trgtTaint,
          t.hash,
          t.blockId,
          (new HashSet[RHOCTxnEdge]() + txn)
        )
      }
    ).toSet
  }

  def adjustmentsMap( adjustments : List[RHOCTxnEdge] ) : Map[String,( Float, Float, Set[List[RHOCTxnEdge]] )] = {
    val empty : Set[List[RHOCTxnEdge]] = new HashSet[List[RHOCTxnEdge]]()
    val seed : Map[String,( Float, Float, Set[List[RHOCTxnEdge]] )] = new HashMap[String,( Float, Float, Set[List[RHOCTxnEdge]] )]()
    balances().foldLeft( seed )(
      ( acc, entry ) => {
        val ( addr, balance ) = entry
        val addrCreditAdj : List[RHOCTxnEdge] = adjustments.filter( ( e ) => e.trgt.addr == addr )
        val addrDebitAdj : List[RHOCTxnEdge] = adjustments.filter( ( e ) => e.src.addr == addr )
        ( addrCreditAdj, addrDebitAdj ) match {
          case ( Nil, _ ) => {
            println( s"$addr not in clique" )
            val adj = ( balance, 0.toFloat, empty )
            acc + ( addr -> adj )
          }
          case ( inEdges, outEdges ) => {
            val adjSeed : ( Float, Set[List[RHOCTxnEdge]] ) = ( 0, empty )
            val adj = inEdges.foldLeft( adjSeed )(
              ( adjAcc, e ) => {
                val ( adjAccW, adjAccPaths ) = adjAcc
                ( adjAccW + e.weight, adjAccPaths.asInstanceOf[Set[List[RHOCTxnEdge]]] ++ e.paths().asInstanceOf[Set[List[RHOCTxnEdge]]] )
              }
            )
            val debit = outEdges.foldLeft( 0.asInstanceOf[Float] )( ( dAcc, e ) => { dAcc + e.weight } )
            val totalAdjustment = adj._1 - debit
            println( s"$addr in clique with" )
            println( s"${inEdges.length} incoming edges contributing ${adj._1}") 
            println( s"${outEdges.length} outgoing edges debiting ${debit}" ) 
            println( s"giving total adjustment ${totalAdjustment}" )
            val balAdj = ( balance, totalAdjustment, adj._2 )
            acc + ( addr -> balAdj )
          }
        }
      }
    )
  }

  def reportAdjustmentsMap(
    adjustmentsMap : Map[String,( Float, Float, Set[List[RHOCTxnEdge]] )],
    adjFileName : String, proofFileName : String,
    dir : String
  ) : Unit = {
    val adjFName          = s"${dir}/${adjFileName}"
    val pfFName           = s"${dir}/${proofFileName}"

    // collect stats
    val numberOfAdjustments =
      adjustmentsMap.foldLeft( 0 )(
      ( acc, entry ) => {
        val ( k, ( balance, adjustment, pf ) ) = entry
        if ( adjustment != 0 ) {
          acc + 1
        }
        else acc
      }
    )
    val nonDust =
      adjustmentsMap.foldLeft( ( 0, 0.toFloat, List[(String,Float)]() ) )(
        ( acc, entry ) => {
          val ( k, ( balance, adjustment, pf ) ) = entry
          if ( adjustment > 1 ) {
            val ( count, total, addrs ) = acc
            val biglyHit = ( k, balance )
            ( count + 1, total + adjustment, addrs ++ List[(String,Float)](biglyHit) )
          }
          else acc
        }
      )

    println( s"RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR" )
    println( s"Total number of adjustments ${numberOfAdjustments}" )
    println( s"Out of ${adjustmentsMap.size}" )
    println( s"Total adjustments that are not dust ${nonDust._1} adding to ${nonDust._2}" )
    for( addr <- nonDust._3 ) { println( s"${addr._1}" ) }
    println( s"RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR" )

    // proofs were being written to adjustment file... lol!
    val adjustmentsFile   = new File( adjFName )
    val adjustmentsWriter = new BufferedWriter( new FileWriter( adjustmentsFile ) )
    
    for( ( k, v ) <- adjustmentsMap ) {
      val ( balance, adjustment, proof ) = v
      if ( adjustment > 0.00000001 ) { // RHOC precision
        println( s"${k} -> ${adjustment}" )
        adjustmentsWriter.write( s"$k, ${balance}, ${adjustment}\n" )        
      }      
    }

    adjustmentsWriter.flush()
    adjustmentsWriter.close()

    val proofFile         = new File( pfFName )
    val proofWriter       = new BufferedWriter( new FileWriter( proofFile ) )
    for( ( k, v ) <- adjustmentsMap ) {
      val ( balance, adjustment, proof ) = v
      if ( adjustment > 0.00000001 ) { // RHOC precision
        println( s"${k} -> ${adjustment}" )
        proofWriter.write( s"$k, ${proof}\n" )
      }      
    }

    proofWriter.flush()
    proofWriter.close()
  }

  def reportAdjustmentsMap( adjustmentsMap : Map[String,( Float, Float, Set[List[RHOCTxnEdge]] )] ) : Unit = {
    reportAdjustmentsMap( adjustmentsMap, adjustmentsFile, proofFile, reportingDir )
  }

  def annotateFileName( fNameStr : String, annotation : String ) = {
    val fNameComponents = fNameStr.split( '.' )
    val fName = fNameComponents( 0 )
    val fExt = fNameComponents( 1 )
    s"${fName}${annotation}.${fExt}"
  }

  val BarcelonaWeights = getClique( barcelonaEdge )
  val PithiaWeights = getClique( pithiaEdge )

  def reportClique( clique : List[RHOCTxnEdge] ) : Unit = {
    val cliqueFile = new File( s"clique.csv" )
    val cliqueWriter = new BufferedWriter( new FileWriter( cliqueFile ) )
    for( txn <- clique ) {
      cliqueWriter.write( s"$txn\n" )
    }
    cliqueWriter.flush()
    cliqueWriter.close()
  }

  // Exploration
  def findTxns( clique : List[RHOCTxnEdge] )( addr : String ) : List[RHOCTxnEdge] = {
    clique.filter( ( txn ) => { ( txn.src == addr ) || ( txn.trgt == addr ) } )
  }

  def findTxn( clique : List[RHOCTxnEdge] )( hash : String ) : Option[RHOCTxnEdge] = {
    clique.filter( ( txn ) => { txn.hash == hash } ) match {
      case Nil => None
      case txn :: Nil => Some( txn )
      case txns => throw new Exception( s"more than one transaction with the same hash: $hash \n txns: $txns" )
    }
  }

  def findTxnProof( clique : List[RHOCTxnEdge] )( hash : String ) : Set[List[RHOCTxnEdge]] = {
    findTxn( clique )( hash ) match {
      case None => new HashSet[List[RHOCTxnEdge]]
      case Some( txn ) => txn.paths().asInstanceOf[Set[List[RHOCTxnEdge]]]
    }
  }

  object BarcelonaClique extends JustifiedClosure[Address, RHOCTxnEdge] {
    override def next = nextRHOCTxnTaint( BarcelonaWeights )
    override def key = _.trgt

    def getClique( taintedEdge : RHOCTxnEdge ) : List[RHOCTxnEdge] = { taintedClique( close( taintedEdge ) ) }

    val BarcelonaAdjustments = getClique( barcelonaEdge )

    def reportAdjustments( ) : Unit = {
      val adjFName = annotateFileName( AdjustmentConstants.adjustmentsFile, "Barcelona" )
      val pfFName = annotateFileName( AdjustmentConstants.proofFile, "Barcelona" )

      reportAdjustmentsMap(
        adjustmentsMap( BarcelonaAdjustments ),
        adjFName,
        pfFName,
        reportingDir
      )
    }
  }  

  object PithiaClique extends JustifiedClosure[Address, RHOCTxnEdge] {
    override def next = nextRHOCTxnTaint( PithiaWeights )
    override def key = _.trgt

    def getClique( taintedEdge : RHOCTxnEdge ) : List[RHOCTxnEdge] = { taintedClique( close( taintedEdge ) ) }

    val PithiaAdjustments = getClique( pithiaEdge )

    def reportAdjustments( ) : Unit = {
      val adjFName = annotateFileName( adjustmentsFile, "Pithia" )
      val pfFName = annotateFileName( proofFile, "Pithia" )
      reportAdjustmentsMap(
        adjustmentsMap( PithiaAdjustments ),
        adjFName,
        pfFName,
        reportingDir
      )
    }
  }
}
