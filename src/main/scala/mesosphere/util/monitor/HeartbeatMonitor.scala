package mesosphere.util.monitor

import javax.inject.Inject

import akka.actor._
import akka.event.LoggingReceive
import scala.concurrent.duration._

// HeartbeatActor monitors the heartbeat of some process and executes handlers for various conditions.
// If an expected heartbeat is missed then execute an `onSkipped` handler.
// If X number of subsequent heartbeats are missed then execute an `onFailure` handler.
// Upon creation the actor is in an inactive state and must be sent an EventActivate message to activate.
// Once activated the actor will monitor for EventPulse messages (these are the heartbeats).
// The actor may be deactivated by sending it an EventDeactivate message.
class HeartbeatActor @Inject() (
    config: HeartbeatActor.Config
) extends Actor {

  import HeartbeatActor._

  def receive: Receive = stateInactive

  def stateInactive: Receive = LoggingReceive.withLabel("inactive") {
    case EventActivate(onSkipped, onFailure) =>
      context.become(stateActive(StateActive(onSkipped, onFailure, resetTimer())))
    case _ => // swallow all other event types
  }

  def stateActive(state: StateActive): Receive = LoggingReceive.withLabel("active") {

    case EventPulse =>
      context.become(stateActive(state.copy(missed = 0, timer = resetTimer(state.timer))))

    case EventSkipped =>
      if (state.missed + 1 > config.missedHeartbeatsThreshold) {
        state.onFailure.run
        context.become(stateActive(state.copy(missed = 0, timer = resetTimer(state.timer))))
      } else {
        state.onSkipped.run
        context.become(stateActive(state.copy(missed = state.missed + 1, timer = resetTimer(state.timer))))
      }

    case EventDeactivate =>
      state.timer.foreach(_.cancel)
      context.become(stateInactive)

    case EventActivate(updatedSkipped, updatedFailure) =>
      context.become(stateActive(state.copy(missed = 0, timer = resetTimer(state.timer),
        onSkipped = updatedSkipped, onFailure = updatedFailure)))
  }

  protected def resetTimer(timer: Option[Cancellable] = None): Option[Cancellable] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // cancel current timer (if any) and send EventSkipped after duration specified by heartbeatTimeout
    timer.foreach(_.cancel)
    Some(config.system.scheduler.scheduleOnce(config.heartbeatTimeout, self, EventSkipped))
  }
}

object HeartbeatActor {

  case class Config(
    system: ActorSystem,
    heartbeatTimeout: FiniteDuration,
    missedHeartbeatsThreshold: Int)

  sealed trait Event
  case object EventPulse extends Event
  case object EventSkipped extends Event
  case object EventDeactivate extends Event
  case class EventActivate(heartbeatSkippedAction: Runnable, heartbeatFailureAction: Runnable) extends Event

  case class StateActive(
    onSkipped: Runnable,
    onFailure: Runnable,
    timer: Option[Cancellable] = None,
    missed: Int = 0)

  def props(config: Config): Props = Props(classOf[HeartbeatActor], config)
}
