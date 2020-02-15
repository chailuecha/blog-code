package cc.home.msg.dedup

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit

/*  Simple producer to send a number of messages for testing */
fun main(args: Array<String>) {
    val producer: KafkaProducer<String, String> = createProducer()

    val numMessage = 2000
    for (i in 1..numMessage) {
        val key = UUID.randomUUID().toString()
        producer.send(ProducerRecord(MESSAGES_TOPIC, key, key))

        if (i % 100 == 0) {
            TimeUnit.MILLISECONDS.sleep(100)
        }
    }

    producer.flush()
    producer.close()

    println("Done sending $numMessage messages")
}

fun createProducer(): KafkaProducer<String, String> {
    val config = Properties().apply {
        setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        setProperty(ProducerConfig.ACKS_CONFIG, "all")
        setProperty(ProducerConfig.LINGER_MS_CONFIG, "100")
        setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    }

    return KafkaProducer(config)
}
