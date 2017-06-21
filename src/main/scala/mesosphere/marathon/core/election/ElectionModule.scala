package mesosphere.marathon
package core.election

import akka.actor.{ ActorSystem, Cancellable }
import akka.event.EventStream
import akka.stream.scaladsl.Source
import mesosphere.marathon.core.base.{ CrashStrategy, LifecycleState }

class ElectionModule(
    config: MarathonConf,
    system: ActorSystem,
    eventStream: EventStream,
    hostPort: String,
    lifecycleState: LifecycleState,
    crashStrategy: CrashStrategy) {

  lazy private val electionBackend: Source[LeadershipState, Cancellable] = if (config.highlyAvailable()) {
    config.leaderElectionBackend.get match {
      case Some("curator") =>
        CuratorElectionStream(config, hostPort)(system)
      case backend: Option[String] =>
        throw new IllegalArgumentException(s"Leader election backend $backend not known!")
    }
  } else {
    PsuedoElectionStream()
  }

  private def onSuicide(): Unit = {
    crashStrategy.crash()
  }

  lazy val service: ElectionService = new ElectionServiceImpl(hostPort, electionBackend, crashStrategy)(system)
}
