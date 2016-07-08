package mesosphere.util.monitor

import akka.actor._
import akka.event.LoggingReceive
import scala.concurrent.duration._

// HeartbeatActor monitors the heartbeat of some process and executes handlers for various conditions.
// If an expected heartbeat is missed then execute an `onSkip` handler.
// If X number of subsequent heartbeats are missed then execute an `onFailure` handler and become inactive.
// Upon creation the actor is in an inactive state and must be sent a MessageActivate message to activate.
// Once activated the actor will monitor for MessagePulse messages (these are the heartbeats).
// The actor may be forcefully deactivated by sending it an MessageDeactivate message.
class HeartbeatActor(config: HeartbeatActor.Config) extends Actor with ActorLogging {

  import HeartbeatActor._

  def receive: Receive = stateInactive

  def stateInactive: Receive = LoggingReceive.withLabel("inactive") {
    case MessageActivate(reactor, token) =>
      context.become(stateActive(StateActive(reactor, token, resetTimer())))
    case _ => // swallow all other event types
  }

  def stateActive(state: StateActive): Receive = LoggingReceive.withLabel("active") {

    case MessagePulse =>
      context.become(stateActive(state.copy(missed = 0, timer = resetTimer(state.timer))))

    case MessageSkipped =>
      if (state.missed + 1 > config.missedHeartbeatsThreshold) {
        state.reactor.onFailure
        context.become(stateInactive)
      } else {
        state.reactor.onSkip
        context.become(stateActive(state.copy(missed = state.missed + 1, timer = resetTimer(state.timer))))
      }

    case MessageDeactivate(token) =>
      // only deactivate if token == state.sessionToken
      if (token.eq(state.sessionToken)) {
        state.timer.foreach(_.cancel)
        context.become(stateInactive)
      }

    case MessageActivate(newReactor, newToken) =>
      context.become(stateActive(StateActive(
        timer = resetTimer(state.timer), reactor = newReactor, sessionToken = newToken)))
  }

  protected def resetTimer(timer: Option[Cancellable] = None): Option[Cancellable] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // cancel current timer (if any) and send MessageSkipped after duration specified by heartbeatTimeout
    timer.foreach(_.cancel)
    Some(config.system.scheduler.scheduleOnce(config.heartbeatTimeout, self, MessageSkipped))
  }
}

object HeartbeatActor {

  case class Config(
    system: ActorSystem,
    heartbeatTimeout: FiniteDuration,
    missedHeartbeatsThreshold: Int)

  sealed trait Message
  case object MessagePulse extends Message
  case object MessageSkipped extends Message
  case class MessageDeactivate(sessionToken: AnyRef) extends Message
  case class MessageActivate(reactor: Reactor, sessionToken: AnyRef) extends Message

  trait Reactor {
    def onSkip(): Unit
    def onFailure(): Unit
  }

  /**
    * @constructor capture the state of an active heartbeat monitor
    */
  case class StateActive(
    reactor: Reactor,
    sessionToken: AnyRef,
    timer: Option[Cancellable] = None,
    missed: Int = 0)

  def props(config: Config): Props = Props(classOf[HeartbeatActor], config)
}
