package cc.home.msg.dedup.consumer

import cc.home.msg.dedup.MESSAGES_TOPIC
import cc.home.msg.dedup.ROCKS_DB_FILE
import cc.home.msg.dedup.getDuplicateCount
import cc.home.msg.dedup.incrementDuplicateCount
import cc.home.msg.dedup.simulateTooLongProcessing
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

@EnableConfigurationProperties(ConsumerApplicationConfig::class)
@SpringBootApplication
class AutoCommitConsumer(config: ConsumerApplicationConfig) : CommandLineRunner {
    private val logger = KotlinLogging.logger { }
    private val consumer: KafkaConsumer<String, String>

    init {
        val kafkaConfig = config.getKafkaConfig().apply {
            setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
            setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000")
        }

        consumer = KafkaConsumer(kafkaConfig)
    }

    override fun run(vararg args: String) {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();

        Options().setCreateIfMissing(true).use { options ->
            RocksDB.open(options, ROCKS_DB_FILE).use { db ->
                consumeWithDeduplication(db)
            }
        }
    }

    fun consumeWithDeduplication(db: RocksDB) {
        //Let any exception propagate up and stop main thread

        consumer.subscribe(listOf(MESSAGES_TOPIC))

        val count = AtomicInteger(0)
        while (true) {
            logger.debug { "Poll next batch of records" }
            val records = consumer.poll(Duration.ofSeconds(2))
            logger.debug { "Polled ${count.addAndGet(records.count())} records. Duplicate count: ${db.getDuplicateCount()}" }

            records.forEach { rec ->
                val keyBytes = rec.key().toByteArray()
                when (db.get(keyBytes)) {
                    null -> db.put(keyBytes, keyBytes)
                    else -> db.incrementDuplicateCount()
                }
            }

            simulateTooLongProcessing()
        }
    }
}


