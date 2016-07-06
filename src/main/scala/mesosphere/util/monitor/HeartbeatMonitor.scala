package mesosphere.util.monitor

import akka.actor._
import akka.event.LoggingReceive
import scala.concurrent.duration._

class HeartbeatActor(config: HeartbeatActor.Config) extends Actor {

  import HeartbeatActor._

  private[this] var state: StateFn = stateInactive

  def receive: Receive = LoggingReceive.withLabel(config.label) {
    case e: Event =>
      state = state(self, e)
  }

  def stateInactive: StateFn = new StateFn {

    self =>

    def apply(actor: ActorRef, e: Event): StateFn =
      e match {
        case EventActivate(onSkipped, onFailure) => stateActive(actor, onSkipped, onFailure)
        case _ => self // swallow all other event types
      }
  }

  def stateActive(actor: ActorRef, onSkipped: Runnable, onFailure: Runnable): StateFn = new StateFn {

    self =>

    var timer: Option[Cancellable] = None
    var missed: Int = 0
    var skippedHandler = onSkipped
    var failureHandler = onFailure

    // scalastyle:off return
    def apply(actor: ActorRef, e: Event): StateFn = {
      e match {
        case EventPulse =>
          missed = 0

        case EventSkipped =>
          missed += 1
          if (missed > config.missedHeartbeatsThreshold) {
            failureHandler.run
            missed = 0
          } else {
            skippedHandler.run
          }

        case EventDeactivate =>
          timer.foreach(_.cancel)
          return stateInactive

        case EventActivate(updatedSkipped, updatedFailure) =>
          missed = 0
          skippedHandler = updatedSkipped
          failureHandler = updatedFailure
      }

      resetTimer(actor)
      self
    }
    // scalastyle:on return

    def resetTimer(actor: ActorRef): Unit = {
      import scala.concurrent.ExecutionContext.Implicits.global

      timer.foreach(_.cancel)
      timer = Some(config.system.scheduler.scheduleOnce(config.heartbeatTimeout, actor, EventSkipped))
    }

    resetTimer(actor)
  }
}

object HeartbeatActor {

  case class Config(
    label: String,
    system: ActorSystem,
    heartbeatTimeout: FiniteDuration,
    missedHeartbeatsThreshold: Int)

  sealed trait Event
  case object EventPulse extends Event
  case object EventSkipped extends Event
  case object EventDeactivate extends Event
  case class EventActivate(heartbeatSkippedAction: Runnable, heartbeatFailureAction: Runnable) extends Event

  trait StateFn extends Function2[ActorRef, Event, StateFn]

  def props(config: Config): Props = Props(classOf[HeartbeatActor], config)
}
