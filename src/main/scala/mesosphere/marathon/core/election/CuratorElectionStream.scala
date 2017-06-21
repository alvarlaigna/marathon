package mesosphere.marathon
package core.election

import akka.actor.{ ActorSystem, Cancellable }
import akka.stream.scaladsl.{ Source, SourceQueueWithComplete }
import akka.stream.OverflowStrategy
import com.typesafe.scalalogging.StrictLogging
import java.util
import java.util.Collections
import java.util.concurrent.{ ExecutorService, Executors, TimeUnit }
import mesosphere.marathon.metrics.{ Metrics, ServiceMetric, Timer }
import mesosphere.marathon.util.CancellableOnce
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.{ ACLProvider, UnhandledErrorListener }
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.recipes.leader.{ LeaderLatch, LeaderLatchListener }
import org.apache.curator.framework.{ AuthInfo, CuratorFrameworkFactory }
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

object CuratorElectionStream extends StrictLogging {
  /**
    * Connects to Zookeeper and offers leadership; monitors leader state. Watches for leadership changes (leader
    * changed, was elected leader, lost leadership), and emits events accordingly.
    *
    * Materialized cancellable is used to abdicate leadership; which will do so followed by a closing of the stream.
    */
  def apply(config: ZookeeperConf, hostPort: String)(implicit actorSystem: ActorSystem): Source[LeadershipState, Cancellable] = {
    val leaderEvents = Source.queue[LeadershipState](16, OverflowStrategy.dropHead).mapMaterializedValue { sq =>

      try {
        val client = newCuratorConnection(config)
        val emitterLogic = new CuratorEventEmitter(client, config.zooKeeperLeaderPath, hostPort, sq)
        emitterLogic.start()
      } catch {
        case ex: Throwable =>
          sq.fail(ex)
      }

      new CancellableOnce(() => sq.complete())
    }

    // When the stream closes, we want the last word to be that there is no leader
    leaderEvents
      .initialTimeout(config.zooKeeperConnectionTimeout().millis)
      .concat(Source.single(LeadershipState.Standby(None)))
  }

  private class CuratorEventEmitter(
      client: CuratorFramework,
      zooKeeperLeaderPath: String,
      hostPort: String, sq: SourceQueueWithComplete[LeadershipState])(implicit actorSystem: ActorSystem) {
    private lazy val threadExecutor: ExecutorService = Executors.newSingleThreadExecutor()
    private implicit lazy val singleThreadEC: ExecutionContext = ExecutionContext.fromExecutor(threadExecutor)
    private lazy val leaderHostPortMetric: Timer = Metrics.timer(ServiceMetric, getClass, "current-leader-host-port")
    private lazy val latch = new LeaderLatch(client, zooKeeperLeaderPath + "-curator", hostPort)

    @volatile private var _isLeader: Boolean = false
    @volatile private var _currentLeader: Option[String] = None
    @volatile private var pollCancellable: Option[Cancellable] = None
    @volatile private var started = false
    @volatile private var stopped = false

    private def getCurrentLeader: Option[String] = leaderHostPortMetric.blocking {
      if (client.getState == CuratorFrameworkState.STOPPED)
        None
      else {
        try {
          val participant = latch.getLeader
          if (participant.isLeader) Some(participant.getId) else None
        } catch {
          case ex: Throwable =>
            logger.error("Error while getting current leader", ex)
            None
        }
      }
    }

    private val listener: LeaderLatchListener = new LeaderLatchListener {
      override def notLeader(): Unit = {
        _isLeader = true
        _currentLeader = getCurrentLeader
        sq.offer(LeadershipState.Standby(_currentLeader))
        logger.info(s"Leader defeated. New leader: ${_currentLeader.getOrElse("-")}")
      }

      override def isLeader(): Unit = {
        _isLeader = true
        sq.offer(LeadershipState.ElectedAsLeader)
      }
    }

    def start(): Unit = synchronized {
      assert(!started, "already started")
      started = true
      // Register hook such that this logic module will tear itself down if the stream stops for any reason
      sq.watchCompletion().onComplete { _ => stop() }

      try {
        latch.addListener(listener, threadExecutor)
        latch.start()

        // Note: runs in same executor as listener; therefore serial
        pollCancellable = Some(actorSystem.scheduler.schedule(1.seconds, 1.second){
          if (!_isLeader) {
            val nextLeader = getCurrentLeader
            if (nextLeader != _currentLeader) {
              logger.info(s"New leader: ${nextLeader.getOrElse("-")}")
              _currentLeader = nextLeader
              sq.offer(LeadershipState.Standby(nextLeader))
            }
          }
        })

      } catch {
        case ex: Throwable =>
          sq.fail(ex)
      }

    }

    private[this] def stop(): Unit = synchronized {
      assert(!stopped, "already stopped")
      stopped = true
      pollCancellable.foreach(_.cancel())
      pollCancellable = None
      latch.removeListener(listener)

      logger.info("Closing leader latch")
      latch.close()
      logger.info("Leader latch closed")

      logger.info("Asking threadExecutor to shutdown")
      threadExecutor.shutdown()
    }
  }

  private def newCuratorConnection(config: ZookeeperConf) = {
    logger.info(s"Will do leader election through ${config.zkHosts}")

    // let the world read the leadership information as some setups depend on that to find Marathon
    val defaultAcl = new util.ArrayList[ACL]()
    defaultAcl.addAll(config.zkDefaultCreationACL)
    defaultAcl.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)

    val aclProvider = new ACLProvider {
      override def getDefaultAcl: util.List[ACL] = defaultAcl
      override def getAclForPath(path: String): util.List[ACL] = defaultAcl
    }

    val retryPolicy = new ExponentialBackoffRetry(1.second.toMillis.toInt, 10)
    val builder = CuratorFrameworkFactory.builder().
      connectString(config.zkHosts).
      sessionTimeoutMs(config.zooKeeperSessionTimeout().toInt).
      connectionTimeoutMs(config.zooKeeperConnectionTimeout().toInt).
      aclProvider(aclProvider).
      retryPolicy(retryPolicy)

    // optionally authenticate
    val client = (config.zkUsername, config.zkPassword) match {
      case (Some(user), Some(pass)) =>
        builder.authorization(Collections.singletonList(
          new AuthInfo("digest", (user + ":" + pass).getBytes("UTF-8"))))
          .build()
      case _ =>
        builder.build()
    }

    val listener = new LastErrorListener
    client.getUnhandledErrorListenable().addListener(listener)
    client.start()
    if (!client.blockUntilConnected(config.zkTimeoutDuration.toMillis.toInt, TimeUnit.MILLISECONDS)) {
      // If we couldn't connect, throw any errors that were reported
      listener.lastError.foreach { e => throw e }
    }

    client.getUnhandledErrorListenable().removeListener(listener)
    client
  }

  private class LastErrorListener extends UnhandledErrorListener {
    private[this] var _lastError: Option[Throwable] = None
    override def unhandledError(message: String, e: Throwable): Unit = {
      _lastError = Some(e)
    }

    def lastError = _lastError
  }
}
