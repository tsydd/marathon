package mesosphere.marathon.util.heartbeat

import akka.actor._
import scala.concurrent.duration._

// HeartbeatActor monitors the heartbeat of some process and executes handlers for various conditions.
// If an expected heartbeat is missed then execute an `onSkip` handler.
// If X number of subsequent heartbeats are missed then execute an `onFailure` handler and become inactive.
// Upon creation the actor is in an inactive state and must be sent a MessageActivate message to activate.
// Once activated the actor will monitor for MessagePulse messages (these are the heartbeats).
// The actor may be forcefully deactivated by sending it an MessageDeactivate message.
class HeartbeatActor(config: Heartbeat.Config) extends LoggingFSM[HeartbeatInternal.State, HeartbeatInternal.Data] {
  import Heartbeat._
  import HeartbeatInternal._

  startWith(StateInactive, DataNone)

  when(StateInactive) {
    case Event(MessageActivate(reactor, token), DataNone) =>
      goto(StateActive) using DataActive(reactor, token)
    case _ =>
      stay // swallow all other event types
  }

  when(StateActive, stateTimeout = config.heartbeatTimeout) {
    case Event(MessagePulse, data: DataActive) =>
      stay using data.copy(missed = 0)

    case Event(StateTimeout, data: DataActive) =>
      if (data.missed + 1 >= config.missedHeartbeatsThreshold) {
        data.reactor.onFailure
        goto(StateInactive) using DataNone
      } else {
        data.reactor.onSkip
        stay using data.copy(missed = data.missed + 1)
      }

    case Event(MessageDeactivate(token), data: DataActive) =>
      // only deactivate if token == data.sessionToken
      if (token.eq(data.sessionToken)) {
        goto(StateInactive) using DataNone
      } else {
        stay
      }

    case Event(MessageActivate(newReactor, newToken), data: DataActive) =>
      stay using DataActive(reactor = newReactor, sessionToken = newToken)
  }

  whenUnhandled{
    case Event(e, d) =>
      log.warning("unhandled event {} in state {}/{}", e, stateName, d)
      stay
  }

  initialize()
}

object Heartbeat {
  case class Config(
    heartbeatTimeout: FiniteDuration,
    missedHeartbeatsThreshold: Int)

  sealed trait Message
  case object MessagePulse extends Message
  case class MessageDeactivate(sessionToken: AnyRef) extends Message
  case class MessageActivate(reactor: Reactor, sessionToken: AnyRef) extends Message

  trait Reactor {
    def onSkip(): Unit
    def onFailure(): Unit
  }

  def props(config: Config): Props = Props(classOf[HeartbeatActor], config)
}

protected[heartbeat] object HeartbeatInternal {
  import Heartbeat._

  sealed trait State
  case object StateInactive extends State
  case object StateActive extends State

  sealed trait Data
  case object DataNone extends Data

  /** @constructor capture the state of an active heartbeat monitor */
  case class DataActive(
    reactor: Reactor,
    sessionToken: AnyRef,
    missed: Int = 0) extends Data
}
