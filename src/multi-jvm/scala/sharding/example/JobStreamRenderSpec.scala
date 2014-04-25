package sharding.example

import akka.remote.testkit.{MultiNodeSpec, MultiNodeConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.apache.commons.io.FileUtils
import java.io.File
import akka.actor.{ActorIdentity, Identify, Props}
import akka.persistence.journal.leveldb.{SharedLeveldbJournal, SharedLeveldbStore}
import akka.remote.testconductor.RoleName
import akka.cluster.Cluster
import akka.contrib.pattern.ClusterSharding
import akka.testkit.ImplicitSender
import sharding.example.JobStreamRender.{Subscribe, Init}
import scala.concurrent.duration._


object JobStreamRenderConfig extends MultiNodeConfig {
   val persistenceStoreNode = role("db")
   val node1 = role("node1")
   val node2 = role("node2")
   val node3 = role("node3")

   commonConfig(ConfigFactory.parseString("""
     akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
     akka.persistence.journal.plugin = "akka.persistence.journal.leveldb-shared"
     akka.persistence.journal.leveldb-shared.store {
       native = off
       dir = "target/test-shared-journal"
     }
     akka.persistence.snapshot-store.local.dir = "target/test-snapshots"
   """))

  testTransport(true)

}

class JobStreamRenderSpecMultiJvmNode0 extends JobStreamRenderSpec
class JobStreamRenderSpecMultiJvmNode1 extends JobStreamRenderSpec
class JobStreamRenderSpecMultiJvmNode2 extends JobStreamRenderSpec
class JobStreamRenderSpecMultiJvmNode3 extends JobStreamRenderSpec


class JobStreamRenderSpec extends MultiNodeSpec(JobStreamRenderConfig)
  with WordSpecLike with Matchers with BeforeAndAfterAll with ImplicitSender {

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()

  override def initialParticipants = roles.size

  private val storageLocations = List(
    "akka.persistence.journal.leveldb.dir",
    "akka.persistence.journal.leveldb-shared.store.dir",
    "akka.persistence.snapshot-store.local.dir").map(s => new File(system.settings.config.getString(s)))


  import JobStreamRenderConfig._

  //setting up the persistence store
  override protected def atStartup() {
    runOn(persistenceStoreNode) {
      storageLocations.foreach(dir => FileUtils.deleteDirectory(dir))
    }
  }

  override protected def afterTermination() {
    runOn(persistenceStoreNode) {
      storageLocations.foreach(dir => FileUtils.deleteDirectory(dir))
    }
  }


  "JobStream render app" must {

    "startup the shared journal for the jobstream" in {

      runOn(persistenceStoreNode) {
        system.actorOf(Props[SharedLeveldbStore], "store")
      }
      enterBarrier("persistence-actor-started")

      //now setup the nodes that is going to use this journal actor for persisting messages
      runOn(node1, node2) {
        val ref1 = node(persistenceStoreNode) / "user" / "store"
        system.actorSelection(ref1) ! Identify(None)
        val sharedStore = expectMsgType[ActorIdentity].ref.get
        SharedLeveldbJournal.setStore(sharedStore, system)
      }
      enterBarrier("persistence-started")
    }

    "startup the Shards" in within(15.seconds) {
       join(node1, node1)
       join(node2, node1)
       join(node3, node1)
       enterBarrier("sharding started")
    }

    "Run job stream render on node1 and subscribe from node3 " in within(15.seconds) {
      runOn(node1) {
        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
        region ! Init(100L)
      }
      enterBarrier("node1-jobstream-started")

      runOn(node3) {
        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
        awaitAssert {
          within(1.second) {
            region ! Subscribe(100, "hey")
            expectMsg("Here you go...100")
          }
        }
      }
      enterBarrier("node3-subscribed-to-node1")
    }

    "Run job stream render on node2 and subscribe from node3 " in within(15.seconds) {
      runOn(node2) {
        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
        region ! Init(200L)
      }
      enterBarrier("node2-jobstream-started")

      runOn(node3) {
        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
        awaitAssert {
          within(1.second) {
            region ! Subscribe(200, "hey")
            expectMsg("Here you go...200")
          }
        }
      }
      enterBarrier("node3-subscribed-to-node2")
    }
//
//    "job stream render should survive node crashes" in within(15.seconds) {
//      runOn(persistenceStoreNode) {
//        testConductor.shutdown(node1).await
//      }
//      enterBarrier("node1-is-down")
//
//      runOn(node3) {
//        val region = ClusterSharding(system).shardRegion(JobStreamRender.shardName)
//        awaitAssert {
//          within(2.second) {
//            region ! Subscribe(100, "hey")
//            expectMsg("Here you go...100")
//          }
//        }
//      }
//      enterBarrier("jobstream-100-now-running-on-other-node")
//    }
  }

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system) join node(to).address
      startSharding()
    }
    enterBarrier(from.name + "-joined")
  }

  private def startSharding(): Unit = {
    ClusterSharding(system).start(
      typeName = JobStreamRender.shardName,
      entryProps = Some(JobStreamRender.props()),
      idExtractor = JobStreamRender.idExtractor,
      shardResolver = JobStreamRender.shardResolver)
  }

}
