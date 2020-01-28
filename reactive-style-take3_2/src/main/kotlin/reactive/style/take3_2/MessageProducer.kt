package reactive.style.take3_2

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.stereotype.Service
import java.util.Properties
import java.util.UUID
import java.util.concurrent.Executors
import javax.annotation.PreDestroy

@Service
class MessageProducer {
    private val producer: KafkaProducer<String, String>
    private val mapper: ObjectMapper = jacksonObjectMapper()

    init {
        val config = Properties().apply {
            setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            setProperty(ProducerConfig.LINGER_MS_CONFIG, "100")
            setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        }

        producer = KafkaProducer(config)
    }

    fun startSendingMessages(numUser: Int, subPerUser: Int) {
        val exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())

        //Concurrently send messages so one batch of consumer records contains messages of multiple users
        (1..numUser).forEach { count ->
            exec.submit { generateUserMessages("User-$count", subPerUser) }
        }
    }

    private fun generateUserMessages(userName: String, subPerUser: Int) {
        val userId = UUID.randomUUID()
        producer.send(newCreateUserMessage(
                userId = userId,
                email = "$userId.@gmail.com",
                name = userName))
            .get()

        (1..subPerUser).forEach { subId ->
            producer.send(newSubscriptionMessage(userId, subId))
        }
    }

    private fun newCreateUserMessage(userId: UUID, email: String, name: String): ProducerRecord<String, String> {
        return ProducerRecord(COMMAND_TOPIC,
                userId.toString(),
                Message(userId = userId,
                        userDetail = UserDetail(email = email, name = name),
                        subscription = null
                ).serialize()
        )
    }

    private fun newSubscriptionMessage(userId: UUID, subscriptionId: Int): ProducerRecord<String, String> {
        return ProducerRecord(COMMAND_TOPIC,
                userId.toString(),
                Message(
                        userId = userId,
                        userDetail = null,
                        subscription = Subscription(newsLetterId = subscriptionId)
                ).serialize()
        )
    }

    private fun Message.serialize(): String = mapper.writeValueAsString(this)

    @PreDestroy
    fun close() {
        producer.close()
    }
}
