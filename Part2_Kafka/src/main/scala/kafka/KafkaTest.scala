package kafka

import io.circe.generic.auto._
import io.circe.parser._
import org.apache.kafka.clients.consumer.ConsumerConfig
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes.Long
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import java.util.Properties
import Domain._

object KafkaStreamsApp {
  import org.apache.kafka.streams.scala.ImplicitConversions._

  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes
    val deserializer = (aAsBytes: Array[Byte]) => {
      val aAsString = new String(aAsBytes)
      val aOrError = decode[A](aAsString)
      aOrError match {
        case Right(a) => Option(a)
        case Left(error) =>
          println(s"There was an error converting the message $aOrError, $error")
          Option.empty
      }
    }
    Serdes.fromFn[A](serializer, deserializer)
  }

  val builder = new StreamsBuilder

  def top(): Unit = {
    val usersStream: KStream[AccountId, Users] = builder.stream[AccountId, Users](UsersTopic)

    // The stream of Users if fetched, flattened to fit the Fact_User schema and sent to the next topic (which will put it into a table)
    // I could have done a table instead of stream, but the question implies to turn it into a table after loading.
    val FactUsersTable = usersStream.mapValues(user =>
      (user.accountId,
        user.attributes.getOrElse("account_id", ""),
        user.attributes.getOrElse("account_status", ""),
        user.attributes.getOrElse("billing_end_date", ""),
        user.attributes.getOrElse("billing_plan_id", ""),
        user.attributes.getOrElse("is_safari", ""),
        user.attributes.getOrElse("is_windows", ""),
        user.attributes.getOrElse("last_name", ""),
        user.attributes.getOrElse("os", ""),
        user.attributes.getOrElse("user_id", ""),
        user.billing_plan_category,
        user.billing_plan_id,
        user.billing_plan_renewal_type,
        user.is_mobile,
        user.rule_key,
        user.variables.getOrElse("plugin_variable", ""),
        user.visitor_id))

    FactUsersTable.to(FactUsersTopic)
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "5000") //if 5k is reached it will fetch, if not it will wait 2 minutes
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "120000")

    top()

    val topology: Topology = builder.build()

    val application: KafkaStreams = new KafkaStreams(topology, props)
    application.start()
  }
}
