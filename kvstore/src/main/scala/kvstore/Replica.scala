package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  override def preStart(): Unit = {
    arbiter ! Join
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception => Restart
  }

  // similar to Replicator.acks, to track if the persist ack is confirmed
  var primaryPersistAcks = Map.empty[Long, (ActorRef, Cancellable)]
  var secondaryPersistAcks = Map.empty[Long, (ActorRef, String, Cancellable)]

  // Track how many replications are needed in order to confirm replication to be finished
  var replicationAcks = Map.empty[Long, (ActorRef, Long)]
  // Track what are the msg ids for each replicator, to help waive unwanted acks(ids)
  var replicatorAcks = Map.empty[ActorRef, Set[Long]].withDefault(_ => Set.empty[Long])

  private var replicatorReplcateCount = 0L
  private var expectedSeq = 0L
  var persistence: ActorRef = context.actorOf(persistenceProps)

  def receive: Receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv += key -> value
      handleLeaderOnChange(id, key, Some(value))

    case Remove(key, id) =>
      kv -= key
      handleLeaderOnChange(id, key, None)

    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Replicas(replicas) =>
      val others = replicas - self
      val newcomers = others -- secondaries.keySet
      val waivers = secondaries.keySet -- others

      newcomers.foreach(replica => {
        val replicator = context.system.actorOf(Replicator.props(replica))
        secondaries += replica -> replicator
        replicators += replicator
        kv.foreach { case (k, v) =>
          replicator ! Replicate(k, Some(v), replicatorReplcateCount)
          replicatorReplcateCount += 1
        }
      })

      waivers.foreach(replica => {
        secondaries.get(replica) match {
          case Some(replicator) =>
            replicator ! PoisonPill
            secondaries -= replica
            replicators -= replicator

            replicatorAcks.get(replicator) match {
              case Some(outstandingAcks) =>
                // Falsify a `Replicated` msg to waive its expected acks
                outstandingAcks.foreach(id => self ! Replicated("", id))
                replicatorAcks -= replicator
              case None =>
            }
          case None =>
        }
      })

    case Replicated(_, id) =>
      replicationAcks.get(id) match {
        case Some((originator, count)) =>
          if (count > 1) {
            replicationAcks += id -> (originator, count - 1)
          } else {
            // To match the expected number of replication scheduled, including those waived
            replicationAcks -= id
            if (!primaryPersistAcks.contains(id))
              originator ! OperationAck(id)
          }
        case None =>
      }

      replicatorAcks.get(sender()) match {
        case Some(acks) => replicatorAcks += sender() -> acks.excl(id)
        case None =>
      }

    case Persisted(_, id) =>
      primaryPersistAcks.get(id) match {
        case Some((originator, cancellable)) =>
          cancellable.cancel()
          // Local persisted
          primaryPersistAcks -= id
          if (!replicationAcks.contains(id)) {
            // All replication finished
            originator ! OperationAck(id)
          }
        case None =>
      }
  }

  // Works for both `insert` and `remove`
  private def handleLeaderOnChange(id: Long, key:String, valueOption: Option[String]): Unit = {
    primaryPersistAcks += id -> (sender(), context.system.scheduler.scheduleWithFixedDelay(
      0.milliseconds, 100.milliseconds, persistence, Persist(key, valueOption, id)
    ))

    if (replicators.nonEmpty) {
      replicationAcks += id -> (sender(), replicators.size)
      replicators.foreach(replicator => {
        replicatorAcks += replicator -> replicatorAcks(replicator).incl(id)
        replicator ! Replicate(key, valueOption, id)
      })
    }

    context.system.scheduler.scheduleOnce(1.second) {
      primaryPersistAcks.get(id) match {
        case Some((originator, cancellable)) =>
          cancellable.cancel()
          primaryPersistAcks -= id
          originator ! OperationFailed(id)

        case None =>
          replicationAcks.get(id) match {
            case Some((originator, _)) =>
              replicationAcks -= id
              originator ! OperationFailed(id)
            case None =>
          }
      }
    }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOption, seq) =>
      if (expectedSeq == seq) {
        kv = valueOption.fold(kv - key)(kv.updated(key, _))
        secondaryPersistAcks += seq -> (sender(), key,
          context.system.scheduler.scheduleWithFixedDelay(
            0.milliseconds, 100.milliseconds, persistence, Persist(key, valueOption, seq))
        )
        expectedSeq += 1
      } else if (expectedSeq > seq) {
        sender() ! SnapshotAck(key, seq)
      }

    case Persisted(_, seq) =>
      secondaryPersistAcks.get(seq) match {
        case Some((replicator, key, cancellable)) =>
          cancellable.cancel()
          secondaryPersistAcks -= seq
          replicator ! SnapshotAck(key, seq)

        case None =>
      }
  }

}

