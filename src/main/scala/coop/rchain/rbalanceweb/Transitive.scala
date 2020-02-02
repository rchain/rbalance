package coop.rchain.rbalance.transitive
import scala.collection.immutable.Set
import scala.collection.immutable.HashSet
import scala.collection.immutable.Map
import scala.collection.immutable.HashMap

trait Justified[Justification] {
  def justification : Set[_ <: Justified[Justification]]
  def paths( ) : Set[List[_ <: Justified[Justification]]] = {
    justification.size match {
      case 0 => new HashSet[List[_ <: Justified[Justification]]]() + List( this )
      case _ => { justification.flatMap( ( w ) => { w.paths().map( _ ++ List( this ) ) } ) }
    }
  }
}

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

trait JustifiedClosure[Key, JJ <: Justified[JJ]] extends Closure[Key,JJ] {
  def computePaths( src : JJ ) : Map[Key,Set[List[_ <: Justified[JJ]]]] = {
    close( src ).foldLeft( new HashMap[Key,Set[List[_ <: Justified[JJ]]]]() )(
      { ( acc, e ) =>
        {
          val ( k, v ) = e
          acc + ( k -> v.flatMap( ( jj ) => { jj.paths( ) } ) )
        }
      }
    )
  }
}

// Simple test
case class Edge[Node]( 
  src : Node, trgt : Node, 
  override val justification : Set[_ <: Edge[Node]]
) extends Justified[Edge[Node]] {
  override def toString() : String = { s"$src => $trgt" }
}
object GraphClosure extends JustifiedClosure[String,Edge[String]] {
  val g1 : Set[Edge[String]] = {
    val a2b = Edge( "a", "b", new HashSet[Edge[String]]() )
    val a2c = Edge( "a", "c", new HashSet[Edge[String]]() )
    val b2c = Edge( "b", "c", new HashSet[Edge[String]]() + a2b )
    val b2d = Edge( "b", "d", new HashSet[Edge[String]]() + a2b )
    val c2b = Edge( "c", "b", new HashSet[Edge[String]]() + a2c )
    val c2d = Edge( "c", "d", new HashSet[Edge[String]]() + a2c )
    val d2a = Edge( "d", "a", new HashSet[Edge[String]]() + b2d + c2d )
    (new HashSet[Edge[String]]() 
      + a2b + a2c
      + b2c + b2d 
      + c2b + c2d
      + d2a )
  }
  def nextEdges( g : Set[Edge[String]] )( e : Edge[String] ) = {
    g.filter( ( e1 : Edge[String] ) => { ( e1.src == e.trgt ) } )
  }
  def source( s : String ) = Edge( "", s, new HashSet[Edge[String]]() )
  def sink( s : String ) = Edge( s, "", new HashSet[Edge[String]]() )

  override def next = { ( e : Edge[String] ) => nextEdges( g1 )( e ) }
  override def key = _.trgt
}

