package cc.home.msg.dedup.consumer

import cc.home.msg.dedup.TARGET_MAX_POLL_INTERVAL_MS
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import java.util.Properties

@ConstructorBinding
@ConfigurationProperties(prefix = "consumer")
class ConsumerApplicationConfig(
        val bootstrapServers: String,
        val groupId: String
) {
    fun getKafkaConfig(): Properties {
        return Properties().apply {
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500")
            setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, TARGET_MAX_POLL_INTERVAL_MS.toString())
            setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        }
    }
}
