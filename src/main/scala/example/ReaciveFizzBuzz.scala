package example

import scala.concurrent._
import akka._
import akka.actor._
import akka.stream._
import akka.util._

/** Reactive な fizzbuzz Akka Streamを用いて実装された fizzbuzz 。
  */
object ReactiveFizzBuzz extends App {
  // Actorを作るために必要な宣言。
  implicit val system = ActorSystem("NumSys")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  // Source といった諸々のDSLを使えるようにする。
  import akka.stream.scaladsl._

  // GraphDSL を用いて、非直線な Stream を構築する。
  val g = RunnableGraph.fromGraph(GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      // ~> といった DSL を使えるようにする。
      import GraphDSL.Implicits._

      // 1から1000まで出力する source 。
      // throttle 機能により、1秒あたり60入力しか受け付けないように設定したため、適切なペースで出力されていく。
      import scala.concurrent.duration._
      import scala.language.postfixOps
      val src = Source(1 to 1000).throttle(200, 1 second)

      // 入力を3分岐させる。 Int を受け取るので Broadcast[Int] となる。基本的に型パラメータは入力の型を与えればよく、出力の型は自動的に推論される。
      val bcast = builder.add(Broadcast[Int](3))

      // Int を受け取り、それが3で割りきれるなら、 Some("fizz") を、さもなくば None を出力する flow 。 buzzも同様。
      // いったん変数にPartialFunctionを受ける
      type -->[A, B] = PartialFunction[A, B]
      val fizzpf: Int --> String = { case n if n % 3 == 0 => "fizz" }
      val buzzpf: Int --> String = { case n if n % 5 == 0 => "buzz" }
      // liftして Int => Option[String] に変換する
      val List(fizz, buzz) = List(fizzpf.lift, buzzpf.lift).map(Flow[Int].map)

      // 2つの Option[String] を受け取り、結合して Option[String] を出力する ZipWith 。 Zip に加えて、 Tuple2 を 受け取り String を返す Flow の組み合わせでも実現できるが、 ZipWith を使ったほうが簡便。
      // ここでは型パラメータは[入力1, 入力2, 出力]になっている。
      // Semigroup を使ってうまく処理する。
      import cats.Semigroup
      import cats.implicits._
      val zipJoinString =
        builder.add(
          ZipWith((lhs: Option[String], rhs: Option[String]) => lhs |+| rhs)
        )

      // 2つの入力 (lhs, rhs とする) を受け取り、 lhs が非-空文字列ならそれを、さもなくば rhs を出力する Flow 。
      // lhs は "fizz" "buzz" "fizzbuzz" のいずれかの値をとり、 Option にくるまれている。
      // rhs は src から渡ってくる Int を文字列化したものが Some として与えられる。
      // ここでは型パラメータは[入力1, 入力2, 出力]になっている。
      // Zip 系コンポーネントは2つの入力を待機し、それぞれが揃うようにするので、どちらかが欠けることはない。
      // SemigroupK を使ってうまく処理する。
      import cats.SemigroupK
      val zipTakeFirstIfNotEmpty =
        builder.add(
          ZipWith((lhs: Option[String], rhs: Option[String]) =>
            (lhs <+> rhs).get
          )
        )

      // Int を入力に取り、文字列に変形するだけの Flow 。
      // 入力を関数に渡したいので map を使っている。
      // 後で便利に使うのでここで Option[String] に格納している。
      val stringify = Flow[Int].map(_.toString().some)

      // 文字列を入力に取り、それを一定の書式で表示する Sink 。
      val sink = Sink.foreach[String] { elem =>
        // 恐るべきことに、 sink は低頻度でクラッシュしてしまう。何もしなければそこで中断して終了するが、後で実行戦略を設定するため無視して続行される。
        if (Math.random() > 0.99) {
          system.log.error("sink dead")
          throw new RuntimeException("Boom!")
        }
        println(s"got: $elem")
      }

      // ここで、各コンポーネントを結合する。
      // 結合には ~> を使う。 ~> を使うと、自動的に via や to に変換される。
      // bcast は本来は入出力の別に合わせてそれぞれ bcast.in と bcast.out(n) のように書く必要があるが、記述する上で自明な場合は省略できる。
      // さらに、入力と出力を連続させた記述も可能である。
      // zip系は、どの入力先に割り当てたいのかが自明にならないため、 zip.in0 のように明示する必要がある。
      // zip.out も明記する必要があるが、なぜなのかは不明。
      // format: off
      src ~> bcast ~> fizz ~> zipJoinString.in0
             bcast ~> buzz ~> zipJoinString.in1
                              zipJoinString.out ~> zipTakeFirstIfNotEmpty.in0
             bcast ~> stringify                 ~> zipTakeFirstIfNotEmpty.in1
                                                   zipTakeFirstIfNotEmpty.out ~> sink
      // format: on

      // 実行可能な完結したグラフであることを示すために ClosedShape を返す必要がある。
      ClosedShape
  })

  // エラー発生時の回復戦略をここで設定する。
  // throwされたエラーがRuntimeExceptionなら、その要素は捨て、無視して続行する。
  // ただし分岐点にかかわる箇所でこれを行うとデッドロックを誘発するので、使う箇所に注意が必要である。
  val decider: Supervision.Decider = {
    case _: RuntimeException => Supervision.Resume
    case _                   => Supervision.Stop
  }

  g.withAttributes(ActorAttributes.supervisionStrategy(decider)).run()
}
