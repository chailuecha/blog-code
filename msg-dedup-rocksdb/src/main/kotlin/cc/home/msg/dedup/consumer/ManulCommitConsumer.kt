package cc.home.msg.dedup.consumer

import cc.home.msg.dedup.MESSAGES_TOPIC
import cc.home.msg.dedup.ROCKS_DB_FILE
import cc.home.msg.dedup.getDuplicateCount
import cc.home.msg.dedup.incrementDuplicateCount
import cc.home.msg.dedup.simulateTooLongProcessing
import cc.home.msg.dedup.topicPartition
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

@EnableConfigurationProperties(ConsumerApplicationConfig::class)
@SpringBootApplication
class ManualCommitConsumer(config: ConsumerApplicationConfig) : CommandLineRunner {
    private val logger = KotlinLogging.logger { }
    private val consumer: KafkaConsumer<String, String>

    init {
        val kafkaConfig = config.getKafkaConfig().apply {
            setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        }

        consumer = KafkaConsumer(kafkaConfig)
    }

    override fun run(vararg args: String) {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();

        Options().setCreateIfMissing(true).use { options ->
            RocksDB.open(options, ROCKS_DB_FILE).use { db ->
                startPolling(db)
            }
        }
    }

    fun startPolling(db: RocksDB) {
        consumer.subscribe(listOf(MESSAGES_TOPIC))
        val offsetMap = mutableMapOf<TopicPartition, OffsetAndMetadata>()

        while (true) {
            logger.debug { "Poll next batch of records." }
            val records = consumer.poll(Duration.ofSeconds(2))
            logger.debug { "Polled ${records.count()} records." }

            if (records.isEmpty) continue

            val count = AtomicInteger(0)
            records.forEach { rec ->
                val keyBytes = rec.key().toByteArray()
                when (db.get(keyBytes)) {
                    null -> db.put(keyBytes, keyBytes)
                    else -> db.incrementDuplicateCount()
                }

                //Commit the offset of next message to be fetched
                offsetMap[rec.topicPartition()] = OffsetAndMetadata(rec.offset() + 1)
                if (count.incrementAndGet() % 100 == 0) {
                    logger.debug {
                        "Processed 100 messages. Current duplicate count: ${db.getDuplicateCount()}. "
                    }

                    if (count.get() % 300 == 0) simulateTooLongProcessing()

                    logger.debug { "Manually commit offsets." }
                    try {
                        // Commit sync is slow. I use it here just to see if it throw error
                        // when the consumer is already out of consumer group.
                        consumer.commitSync(offsetMap)

                    } catch (ex: CommitFailedException) {
                        logger.error { ex.message }
                    }
                }
            }
        }

    }//startPolling

}


