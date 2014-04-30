package sharding.example

import akka.actor._
import akka.contrib.pattern.{ClusterSharding, ShardRegion}

import akka.routing.RoundRobinPool
import akka.cluster.routing.{ClusterRouterPoolSettings, ClusterRouterPool}
import akka.persistence.{SnapshotOffer, EventsourcedProcessor}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable

object JobStreamRender {

  sealed trait BaseMessage { val jobStreamId: Long }

  case class Subscribe(jobStreamId: Long, payload: Any) extends BaseMessage
  case class Init(jobStreamId: Long) extends BaseMessage
  case class SubscriberCount(jobStreamId: Long) extends BaseMessage

  val idExtractor: ShardRegion.IdExtractor = {
    case s: BaseMessage => (s.jobStreamId.toString, s)
  }

  val shardResolver: ShardRegion.ShardResolver = msg => msg match {
    case s: BaseMessage  => (s.jobStreamId % 1000).toString
  }

  val shardName: String = "JobStreamRender"
  def props(): Props = Props[JobStreamRender]

  private case class AddSubscriber(ref: ActorRef)
  private case class RemoveSubscriber(ref: ActorRef)
  private case object SnapshotTick

}


//Using Akka event sourcing
class JobStreamRender extends EventsourcedProcessor with ActorLogging {

  import JobStreamRender._
  import scala.concurrent.duration._
  import context.dispatcher
  //Starting a schedule ticks to take snapshot of the internal state
  //context.system.scheduler.schedule(100.milliseconds, 2.minutes, self, SnapshotTick)

  var subscribers: mutable.Buffer[ActorRef] = ListBuffer.empty[ActorRef]
  val renderers = context.actorOf(
    ClusterRouterPool(RoundRobinPool(0), ClusterRouterPoolSettings(
      totalInstances = 100, maxInstancesPerNode = 3,
      allowLocalRoutees = false, useRole = None)).props(Props[ActiveSector]),
    name = "renderRouter")

  //Since now we have saving the snapshots, Akka persistence will replay the snapshots and any subscriber messages that are
  //younger than the last snapshot
  override def receiveRecover: Receive = {
    case e: AddSubscriber =>
      addSubscriberAndWatch(e)
    case SnapshotOffer(metadata, subs) =>
      subs.asInstanceOf[List[ActorRef]].foreach(ref => addSubscriberAndWatch(AddSubscriber(ref)))
  }

  override def receiveCommand = {

    case Init(jobStreamId) =>
      println(s"Starting the job stream render ${jobStreamId}")

    case s@Subscribe(jobStreamId, mapping) =>
      println(s"!!!!!!!!!!!!!!!!!!!! Processing ${s}")
      persist(AddSubscriber(sender))(addSubscriberAndWatch)
      renderers.forward(ActiveSector.Draw(jobStreamId))

    case SubscriberCount(jobStreamId) =>
      sender ! subscribers.size

    case Terminated(ref) =>
      persist(RemoveSubscriber(ref))(removeSubscriber)

    case 'Fail =>
      //used to simulate actor crashes
      throw new RuntimeException("!!!! CRASHED")

    case SnapshotTick =>
      //saving the current snapshot of the subscribers. Since the save happens asynchronously we need to make sure
      //we use immutable data
      saveSnapshot(subscribers.toList)
  }

  private def addSubscriberAndWatch(c: AddSubscriber): Unit = {
    subscribers += c.ref
    context.watch(c.ref)
  }

  private def removeSubscriber(c: RemoveSubscriber): Unit = {
    subscribers -= c.ref
  }
}

object ActiveSector {

  case class Draw(jobStreamId: Long)
}

class ActiveSector extends Actor {
  import ActiveSector._

  def receive = {
    case Draw(jobStreamId) =>
       sender ! s"Here you go...${jobStreamId}"
  }
}


class JobStreamRouter extends Actor {
  import scala.concurrent.duration._
  import JobStreamRender._

  context.setReceiveTimeout(100.milliseconds)

  val region = ClusterSharding(context.system).shardRegion(JobStreamRender.shardName)

  def receive: Receive = {
    case s: Subscribe =>
      region.forward(s)

    case ReceiveTimeout =>
      region ! Subscribe(100, "show me a new car")
      region ! Subscribe(100, "show me money")
      region ! Subscribe(200, "surprise me!")
  }
}





