package reactive.style.take3_2

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.GroupedFlux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.PreDestroy

@Service
class MessageConsumer {
    private val consumer: KafkaConsumer<String, String>
    private val pollingThread = Executors.newSingleThreadExecutor()
    private val isStopped = AtomicBoolean()

    private val mapper: ObjectMapper = jacksonObjectMapper()
    private val logger = KotlinLogging.logger { }

    val webClient = WebClient
        .builder()
        .baseUrl("http://localhost:8080")
        .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
        .build()


    init {
        val config = Properties().apply {
            setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
            setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "30")
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        }

        consumer = KafkaConsumer(config)
        consumer.subscribe(listOf(COMMAND_TOPIC))

        pollingThread.submit(::pollLoop)
    }

    private fun pollLoop() {
        while (!isStopped.get()) {
            val records = consumer.poll(Duration.ofSeconds(2))
            logger.debug { "Poll ${records.count()} messages from topic: $COMMAND_TOPIC" }

            if (records.count() != 0) {
                // ====== Un-comment each method and run to see the different.

                /* Bug: using flatmap() with WebClient causing messages
                    to be processed out of order.
                 */
                buggyMessageProcessing(records)

                /* Use concatMap() to process messages one-by-one.
                    But it will be slow
                 */
                //sequentialProcessing(records)

                /* Group messages by key then process each group concurrently.
                    Use concatMap() for messages in same group to maintain correct order.
                 */
                //groupMessagesPerUser(records)
            }
        }
        logger.info { "Exiting consumer polling loop" }
    }

    private fun buggyMessageProcessing(records: ConsumerRecords<String, String>) {
        records
            .toFlux()
            .flatMap { record ->
                record
                    .toMono()
                    .map { mapper.readValue<Message>(record.value()) }
                    .flatMap(::callRemoteWebEndpoint)
            }
            .blockLast()
    }

    private fun sequentialProcessing(records: ConsumerRecords<String, String>) {
        records
            .toFlux()
            .concatMap { record ->
                record
                    .toMono()
                    .map { mapper.readValue<Message>(record.value()) }
                    .flatMap(::callRemoteWebEndpoint)
            }
            .blockLast()
    }

    private fun groupMessagesPerUser(records: ConsumerRecords<String, String>) {
        records
            .toFlux()
            .map { record -> mapper.readValue<Message>(record.value()) }
            .groupBy { record -> record.userId }
            //Use flatMap() to parallely process group of messages.
            .flatMap { messagesPerUser: GroupedFlux<UUID, Message> ->
                messagesPerUser
                    //Use concatMap() to sequentially process each message in same group.
                    .concatMap { msg ->
                        msg.toMono()
                            .doOnNext {
                                when (msg.userDetail != null) {
                                    true -> logger.debug { "Create user: ${msg.userId}" }
                                    else -> logger.debug { "Add subscription for user: ${msg.userId}" }
                                }
                            }
                            .flatMap(::callRemoteWebEndpoint)
                    }
            }
            .blockLast()
    }

    private fun callRemoteWebEndpoint(msg: Message): Mono<Void> {
        return msg
            .toMono()
            .flatMap {
                when {
                    msg.userDetail != null   -> createUser(msg.userId, msg.userDetail)
                    msg.subscription != null -> subscribeNewsLetter(msg.userId, msg.subscription)
                    else                     -> Mono.error(IllegalArgumentException())

                }
            }
            .then() //ignore response body

    }

    private fun createUser(userId: UUID, userDetail: UserDetail): Mono<String> {
        return webClient
            .post()
            .uri("/create-user/$userId")
            .body(BodyInserters.fromValue(userDetail))
            .exchange()
            .transform(::processResponse)
    }

    private fun subscribeNewsLetter(userId: UUID, subscription: Subscription): Mono<String> {
        return webClient
            .post()
            .uri("/subscribe/$userId")
            .body(BodyInserters.fromValue(subscription))
            .exchange()
            .transform(::processResponse)
    }

    private fun processResponse(respMono: Mono<ClientResponse>): Mono<String> {
        return respMono
            .flatMap { resp ->
                //always drain response body to prevent resource leak
                resp.bodyToMono(String::class.java)
                    .doOnNext { body ->
                        when {
                            resp.statusCode() != HttpStatus.OK ->
                                logger.error { "Error in calling web endpoint - $body" }
                        }
                    }
            }
    }

    @PreDestroy
    fun close() {
        isStopped.set(true)
        pollingThread.shutdownNow()
        pollingThread.awaitTermination(30, TimeUnit.SECONDS)
    }
}
