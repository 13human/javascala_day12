package academy.javascala

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.{ActorRef, Cancellable, typed}
import akka.stream.scaladsl.{Balance, Broadcast, Concat, FileIO, Flow, Framing, GraphDSL, Keep, Merge, MergePreferred, RunnableGraph, Sink, Source, StreamConverters, Unzip, UnzipWith, UnzipWith3, Zip, ZipWith, ZipWith2}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._
import akka.util.ByteString
import akka.{Done, NotUsed}

import java.io.{FileInputStream, FileOutputStream}
import java.nio.file.Paths
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.io.StdIn.readLine

object _1Streams extends App {
  val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "as")
  implicit val materializer: Materializer = Materializer(actorSystem)
  Source(1 to 10)
    .via(Flow[Int].map(_ * 2))
    .to(Sink.foreach(println))
    .run()

}

object Sources extends App {
  val actorSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "as")
  implicit val materializer: Materializer = Materializer(actorSystem)

  /**
   * Source - Stage с одним выходом
   * Можно считать началом стрима
   * Может получать данные из файла, базы данных, коллекции
   * Размер данных не задан, может быть бесконечен
   * Тип Source[+Out, +Mat] где Out - тип элементов, а Mat - материализованное значение
   */
  val emptySource: Source[Int, NotUsed] = Source.empty[Int]
  val singleSource: Source[Int, NotUsed] = Source.single(0)
  val repeatSource: Source[String, NotUsed] = Source.repeat("hi")
  val tickSource: Source[String, Cancellable] = Source.tick(
    initialDelay = 1.second,
    interval = 5.seconds,
    tick = "msg"
  )
  val collectionSource: Source[Int, NotUsed] = Source(1 to 10)
  val iteratorSource: Source[Int, NotUsed] = Source.fromIterator(() => Iterator.from(0))
  val cycleSource: Source[Int, NotUsed] = Source.cycle(() => Iterator.range(1, 10))

  val unfoldSource: Source[Int, NotUsed] = Source.unfold(0) {
    case e@small if small < 100 => Option((e + 1, e))
    case _ => None
  }


  val source: Source[Any, ActorRef] = Source.actorRef(
    completionMatcher = {
      case Done =>
        CompletionStrategy.immediately
    },
    failureMatcher = PartialFunction.empty,
    bufferSize = 100,
    overflowStrategy = OverflowStrategy.dropHead)
  val actorRef: ActorRef = source.to(Sink.foreach(println)).run()


  var running = true
  while (running) {
    readLine() match {
      case "stop" => running = false
      case a => actorRef ! a
    }
  }
  actorRef ! Done


  val sourceFromFile: Source[ByteString, Future[IOResult]] = FileIO.fromPath(Paths.get("src/main/resources/application.conf"), chunkSize = 1024)
    .via(Framing.delimiter(ByteString("\n"), 1024))

  sourceFromFile
    .map(_.utf8String)
    .runWith(Sink.foreach(println))

  val interOp: Source[ByteString, Future[IOResult]] = StreamConverters.fromInputStream { () => new FileInputStream("src/main/resources/application.conf") }

  class NumbersSource extends GraphStage[SourceShape[Int]] {
    val out: Outlet[Int] = Outlet("NumbersSource")
    override val shape: SourceShape[Int] = SourceShape(out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        // All state MUST be inside the GraphStageLogic,
        // never inside the enclosing GraphStage.
        // This state is safe to access and modify from all the
        // callbacks that are provided by GraphStageLogic and the
        // registered handlers.
        private var counter = 1

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            push(out, counter)
            counter += 1
          }
        })
      }
  }
}

object Sinks {
  /**
   * Sink - Stage с одним входом
   * Можно считать выходом стрима
   * Может писать данные в файл, базу данных, коллекции
   * Sink создает backpressure
   * Тип Sink[-In, +Mat] где In - тип элементов, а Mat - материализованное значение
   */

  val ignoreSink: Sink[Any, Future[Done]] = Sink.ignore

  val foreachSink: Sink[Int, Future[Done]] = Sink.foreach[Int](i => println(i))

  val headSink: Sink[Int, Future[Int]] = Sink.head[Int]
  // also last, headOption, lastOption

  val toSeq: Sink[Int, Future[Seq[Int]]] = Sink.seq[Int]

  val foldSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0) {
    case (acc, elem) => acc + elem
  }

  val reduceSink: Sink[Int, Future[Int]] = Sink.reduce[Int] {
    case (acc, elem) => acc + elem
  }

  var ref: typed.ActorRef[String] = ???

  import akka.actor.typed.scaladsl.adapter._

  val actorRefSink: Sink[String, NotUsed] = Sink.actorRef[String](ref.toClassic, onCompleteMessage = "stop", onFailureMessage = PartialFunction.empty[Throwable, String])

  val toFile: Sink[ByteString, Future[IOResult]] = FileIO.toPath(Paths.get("src/main/resources/tmp.txt"))

  val toJava: Sink[ByteString, Future[IOResult]] = StreamConverters.fromOutputStream(() => new FileOutputStream("src/main/resources/tmp.txt"))

  class StdoutSink extends GraphStage[SinkShape[Int]] {
    val in: Inlet[Int] = Inlet("StdoutSink")
    override val shape: SinkShape[Int] = SinkShape(in)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        // This requests one element at the Sink startup.
        override def preStart(): Unit = pull(in)

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            println(grab(in))
            pull(in)
          }
        })
      }
  }

  Sink.fromGraph(new StdoutSink)
}

object Flows {
  /**
   * Flow - Stage с одним входом и одним выходом
   * Используется для соединения Source и Sink, манипулирую данными
   * Тип Flow[-In, +Out, +Mat]
   * Может уменьшать backpressure на предыдущие элементы стрима
   */

  val mapFlow: Flow[Int, Int, NotUsed] = Flow[Int].map(i => i * 2)
  val mapAsync: Flow[Int, Int, NotUsed] = Flow[Int].mapAsync(parallelism = 4)(i => Future.successful(i * 2))
  // mapAsyncUnordered

  val mapConcat: Flow[String, String, NotUsed] = Flow[String].mapConcat(str => str.split("\n"))

  val grouped: Flow[Int, Seq[Int], NotUsed] = Flow[Int].grouped(10)

  val sliding: Flow[Int, Seq[Int], NotUsed] = Flow[Int].sliding(10, step = 1)

  val fold: Flow[Int, Int, NotUsed] = Flow[Int].fold(0) {
    case (acc, el) => acc + el
  } // will not emit until upstream completed

  val scan: Flow[Int, Int, NotUsed] = Flow[Int].scan(0) {
    case (acc, el) => acc + el
  } // will emit each computed acc

  val filter: Flow[Int, Int, NotUsed] = Flow[Int].filter(_ % 2 == 1)

  val collect: Flow[Int, Int, NotUsed] = Flow[Int].collect { case x if x % 2 == 0 => x * 2 }

  val takeWithin: Flow[Int, Int, NotUsed] = Flow[Int].takeWithin(1.second) // after terminate

  val dropWithin: Flow[Int, Int, NotUsed] = Flow[Int].dropWithin(1.second)

  val groupBySecond: Flow[Int, Seq[Int], NotUsed] = Flow[Int].groupedWithin(10, 1.second)

  val zip: Flow[String, (String, Int), NotUsed] = Flow[String].zip(Source(1 to 10))

  val flatMapConcat: Flow[Int, Int, NotUsed] = Flow[Int].flatMapConcat(i => Source(0 to i)) // ony by one, order preserved. can stack if any source not terminate

  val flatMapMerge: Flow[Int, Int, NotUsed] = Flow[Int].flatMapMerge(breadth = 2, i => Source(0 to i)) // simultaneous with bradth

  val buffer: Flow[Int, Int, NotUsed] = Flow[Int].buffer(100, OverflowStrategy.backpressure)

  // if consumer faster than producer
  //Flow[Int].expand(i => Iterator(i, i + 1, i + 2))

  // if consumer slower than producer
  //Flow[Int].batch()
  //Flow[Int].conflate()

  val log: Flow[Int, Int, NotUsed] = Flow[Int].log("name") // optinally extract fn

  // Для удобства у Source есть все методы Flow - применяет соответствующий Flow к Source и возвращает новый Source
  val mappedSource: Source[Int, NotUsed] = Source.empty[Int].map(_ * 2)

  class Map[A, B](f: A => B) extends GraphStage[FlowShape[A, B]] {

    val in = Inlet[A]("Map.in")
    val out = Outlet[B]("Map.out")

    override val shape = FlowShape.of(in, out)

    override def createLogic(attr: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            push(out, f(grab(in)))
          }
        })
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            pull(in)
          }
        })
      }
  }
}

object RunnableGraphs extends App {
  // Сами по себе вышестоящие элементы ничего не делают. Их надо объединить в RunnableGraph и материализовать его

  // via - подсоединяет Flow к Source возвращая новый Source
  // to - подсоединяет Sink к Source возвращая RunnableGraph
  // пример в начале
  val rg: RunnableGraph[NotUsed] = Source(1 to 10)
    .via(Flow[Int].map(_ * 2))
    .to(Sink.foreach(println))
  // можно вызывать и на Flow
  val viaFlow: Flow[Int, Int, NotUsed] = Flows.mapFlow.via(Flows.log)
  val toSink: Sink[Int, NotUsed] = viaFlow.to(Sink.ignore)

  /**
   * Materialized Value
   * Каждый этап стрима может производить материализованное значение (+Mat)
   * Когда мы материализуем граф - на его выходе мы можем MV.
   * MV передаются между этапами
   * И мы можем контролировать, что делать с этим MV между этапами
   */
  //implicit val mat: Materializer = Materializer(ActorSystem(Behaviors.empty,"tmp"))
  // No need for implicit Materializer in akka 2.6 just pass implicit system
  implicit val as = ActorSystem(Behaviors.empty, "tmp")
  // to - материализует значение того этапа, где был вызван
  // via - сохраняет MV слева
  // toMat - позволяет выбрать MV - left, right, both, none между исходным этапом и Sink
  // viaMat - тоже самое для Flow
  // run - выполняет граф и возвращает MV

  val source: Source[String, Cancellable] = Source.tick(1.second, 1.second, "hello world")
  val sink: Sink[Any, Future[Done]] = Sink.foreach(println)
  val flow: Flow[String, Array[String], NotUsed] = Flow[String].map(_.split(" "))

  val matSource: Cancellable = source.to(sink).run()
  val matSource2: Cancellable = source.via(flow).to(sink).run()
  val matFlow: NotUsed = source.viaMat(flow)(Keep.right).to(sink).run()
  val matSink: Future[Done] = source.toMat(sink)(Keep.right).run()

  // Shortcuts
  val runWith: Future[Done] = source.runWith(sink)
  val runForeach: Future[Done] = source.runForeach(println) // = Sink.foreach
  val runFold: Future[List[String]] = source.runFold(List.empty[String])(_ :+ _) // = Sink.fold
  val runReduce: Future[Int] = Source(1 to 10).runReduce(_ + _) // = Sink.fold
}

//https://doc.akka.io/docs/akka/current/project/migration-guide-2.5.x-2.6.x.html#akka-stream-changes
object FaultTolerance {
  implicit val as: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "tmp")
  val decider: Supervision.Decider = {
    case _: NoSuchElementException => Supervision.Resume // just skip element saving stage state
    case _ => Supervision.Restart // skips element and restart failed stage
    case _ => Supervision.Stop
  }
  Source(1 to 10).to(Sink.ignore)
    .withAttributes(ActorAttributes.supervisionStrategy(decider))
    .withAttributes(ActorAttributes.dispatcher("my-dispatcher"))
    .withAttributes(ActorAttributes.debugLogging(true))

  // можно вешать per stage
  Flows.filter.withAttributes(ActorAttributes.supervisionStrategy(decider))

  // recover

  Flow[Int].recover {
    case _: ArithmeticException => 0
  }
}

object Graphs extends App {
  // Junctions => Fan-in, Fan-out
  val broadcast: Broadcast[Int] = Broadcast[Int](5)
  val balance: Balance[Int] = Balance[Int](5) // one of available ports
  val unzipWith: UnzipWith3[Int, Int, Int, Int] = UnzipWith((i: Int) => (i, i + 1, i * 2)) // converts one input to N outputs using a functions
  val unzip: Unzip[Int, Int] = Unzip[Int, Int]() // split stream of tuples into each stream

  val merge: Merge[Int] = Merge[Int](3)
  val mergePreferred: MergePreferred[Int] = MergePreferred[Int](3)
  val zip: Zip[Out, Out] = Zip[Int, Int]() // making output tuple from 2 inputs
  type Out = Int
  val zipWith: ZipWith2[Int, Int, Out] = ZipWith((a: Int, b: Int) => a * b)
  val concat = Concat[Int](2) // one before over

  // Как все это соединять??? Graph DSL!
  implicit val system = ActorSystem(Behaviors.empty, "gdsl")
  RunnableGraph.fromGraph(GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val source = Source(1 to 10)
      val sink = Sink.foreach(println)
      val bcast = builder.add(Broadcast[Int](2))
      val merge = builder.add(Merge[Int](2))
      val f1, f2, f3, f4 = Flow[Int].map(_ + 10)

      source ~> f1 ~> bcast ~> f2 ~> merge ~> f4 ~> sink
                      bcast ~> f3 ~> merge
      ClosedShape
  }).run()

  // Partial Graphs
  private val sourceShapeGraph: Graph[SourceShape[Out], NotUsed] = GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
      val source1, source2 = Source(1 to 10)
      val merge = builder.add(Merge[Int](2))
      source1 ~> merge
      source2 ~> merge
      SourceShape(merge.out)
  }
  val source: Source[Out, NotUsed] = Source.fromGraph(sourceShapeGraph)


  val fanInShapeGraph: Graph[UniformFanInShape[Out, Out], NotUsed] = GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      val merge = builder.add(Merge[Int](2))
      UniformFanInShape(
        outlet = merge.out, inlets = merge.in(0), merge.in(1)
      )
  }

  // Simplified Graph API

  val source1, source2 = Source(1 to 10)

  private val mergedSource = Source.combine(source1, source2)(Merge[Int](_))

  val sink1, sink2 = Sink.foreach(println)
  private val mergedSink = Sink.combine(sink1, sink2)(Broadcast[Int](_))

  mergedSource.runWith(mergedSink)


}

object Fusing extends App {
  /**
   * Пересечение асинхронных границ может быть дорогой операцией
   * Поэтому, по умолчанию, Akka Stream объединяет (fusing) все этапы стрима в один синхронный этап.
   * И требует запроцессить один элемент полностью, до того как начнет обрабатываться другой.
   * Это можно отключить конфигом akka.steam.materializer.auto-fusing=off , если в этом есть смысл
   * Так как включая асинхронный режим процессинга элементов, появляется overhead в виде Actors, Mailboxes и Buffers
   */

  /**
   * Зачастую у многих Stage есть маленький буффер
   * Который не используется, когда этап в синхронном режиме
   * При добавление асинхронности эти буфферы начинают использоваться
   */

  // Используя .async можно включить асинхронный режим и отключить Fusing.

  implicit val system = ActorSystem(Behaviors.empty, "fusing")
  Source(1 to 10).async.runForeach(println) // source и sink in fused mode but now they are not connected
}
