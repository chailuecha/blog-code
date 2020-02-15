package cc.home.msg.dedup

import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.rocksdb.RocksDB
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit


private val duplicateCountKey = "dup-count".toByteArray()
const val MESSAGES_TOPIC = "test-message-topic"
const val ROCKS_DB_FILE = "messages.db"

const val TARGET_MAX_POLL_INTERVAL_MS: Long = 10000
private val logger = KotlinLogging.logger { }

fun RocksDB.incrementDuplicateCount() {
    val newCount = getDuplicateCount() + 1
    put(duplicateCountKey, newCount.toByteArray())
}

fun RocksDB.getDuplicateCount(): Int = get(duplicateCountKey)?.toInt() ?: 0

fun RocksDB.clearDuplicateCount() {
    put(duplicateCountKey, 0.toByteArray())
}


fun Int.toByteArray(): ByteArray = ByteBuffer.allocate(4).putInt(toInt()).array()
fun ByteArray.toInt(): Int = ByteBuffer.wrap(this).int

fun ConsumerRecord<String, String>.topicPartition() = TopicPartition(topic(), partition())


fun simulateTooLongProcessing() {
    // Sleep until exceed max poll threshold. Kafka will kick this consumer out of group
    // Commit operation should fail and stop process.
    // Since the offset is not commit properly, subsequent restart should re-consume some duplicated messages
    logger.debug { "Simulate too long processing time." }
    TimeUnit.MILLISECONDS.sleep(TARGET_MAX_POLL_INTERVAL_MS + 20000)
}
