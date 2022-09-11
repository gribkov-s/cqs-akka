import akka.{NotUsed, actor}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Props, _}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, RetentionCriteria}
import akka.{NotUsed, actor}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorSystem, Props, _}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, RetentionCriteria}
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Flow, RunnableGraph, Sink, Source}
import slick.jdbc.H2Profile.api._
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import akka_typed.TypedCalculatorWriteSide._


case class Action(value: Int, name: String)


object akka_typed
{
  trait CborSerializable

  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide {
    sealed trait Command
    case class Add(amount: Int)      extends Command
    case class Multiply(amount: Int) extends Command
    case class Divide(amount: Int)   extends Command

    sealed trait Event
    case class Added(id: Int, amount: Int)      extends Event
    case class Multiplied(id: Int, amount: Int) extends Event
    case class Divided(id: Int, amount: Int)    extends Event
    case class CalculationCompleted(id: Int, amount: Int)    extends Event

    final case class State(value: Int) extends CborSerializable {
      def add(amount: Int): State      = copy(value = value + amount)
      def multiply(amount: Int): State = copy(value = value * amount)
      def divide(amount: Int): State   = copy(value = value / amount)
    }

    object State {
      val empty = State(0)
    }

    def apply(): Behavior[Command] =
      Behaviors.setup { ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
          .snapshotWhen{
          case (state, CalculationCompleted(_, _), seqNumber) if seqNumber % 10 == 0 => true
          case (state, event, seqNumber) => true
        }
          .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 100, keepNSnapshots = 2))
      }

    def handleCommand(
        persistenceId: String,
        state: State,
        command: Command,
        ctx: ActorContext[Command]
    ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"Receive adding for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
            .persist(added)
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"Receive multiplying for number: $amount and state is ${state.value}")
          Effect
            .persist(Multiplied(persistenceId.toInt, amount))
            .thenRun { newState =>
              ctx.log.info(s"The state result is ${newState.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"Receive dividing for number: $amount and state is ${state.value}")
          Effect
            .persist(Divided(persistenceId.toInt, amount))
            .thenRun { x =>
              ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handing event amount is $amount and state is ${state.value}")
          state.divide(amount)
      }
  }

  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed]) {

    implicit val session: SlickSession = SlickSession.forConfig("slick-postgres")
    implicit val materializer: actor.ActorSystem = system.classicSystem

    var (offset, latestCalculatedResult) =
     Await.result(
       session.db.run(
         sql"select write_side_offset, calculated_value from public.result where id = 1".as[(Int, Double)]
       ),
       30.seconds
     ).head

    val startOffset: Int = if (offset == 1) 1 else offset + 1

    val readJournal: CassandraReadJournal =
      PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    val source: Source[EventEnvelope, NotUsed] = readJournal
      .eventsByPersistenceId("001", startOffset, Long.MaxValue)

    val flowUpdateRes: Flow[EventEnvelope, EventEnvelope, NotUsed] = Flow[EventEnvelope].map { event =>
      event.event match {
        case Added(_, amount) =>
          latestCalculatedResult += amount
          println(s"! Log from Added: $latestCalculatedResult")
        case Multiplied(_, amount) =>
          latestCalculatedResult *= amount
          println(s"! Log from Multiplied: $latestCalculatedResult")
        case Divided(_, amount) =>
          latestCalculatedResult /= amount
          println(s"! Log from Divided: $latestCalculatedResult")
      }
      event
    }

    val flowWriteRes: Flow[EventEnvelope, Int, NotUsed] = Slick.flow(event =>
      sqlu"UPDATE public.result SET calculated_value=${latestCalculatedResult}, write_side_offset=${event.sequenceNr} WHERE id= 1 "
    )

    val runnableGraph: RunnableGraph[NotUsed] =
      source.async
      .via(flowUpdateRes).async
      .via(flowWriteRes).async
      .to(Sink.ignore)

    runnableGraph.run()
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup { ctx =>
      val writeActorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calculato", Props.empty)

      writeActorRef ! Add(10)
      writeActorRef ! Multiply(2)
      writeActorRef ! Divide(5)
//
      (1 to 1000).foreach{ x =>
        writeActorRef ! Add(10)
      }
//


      // 0 + 10 = 10
      // 10 * 2 = 20
      // 20 / 5 = 4

      Behaviors.same
    }


  def main(args: Array[String]): Unit = {
    val value = akka_typed()
    implicit val system: ActorSystem[NotUsed] = ActorSystem(value, "akka_typed")

    TypedCalculatorReadSide(system)

    implicit val executionContext = system.executionContext
  }
}
