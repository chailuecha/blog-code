package reactive.style.take3_2

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.router

const val COMMAND_TOPIC = "command-messages"

@SpringBootApplication
class Application(
        private val producer: MessageProducer,
        private val consumer: MessageConsumer
) : CommandLineRunner {

    override fun run(vararg args: String) {
        producer.startSendingMessages(numUser = 100, subPerUser = 5)

        // Observe whether the consumer is processing messages in correct order or not.
        // CTRL-C to exit
    }
}

@Configuration
class AppConfig(val handler: WebRequestHandler) {
    @Bean
    fun route(): RouterFunction<ServerResponse> = router {
        contentType(MediaType.APPLICATION_JSON).nest {
            POST("/create-user/{userId}", handler::createUser)
            POST("/subscribe/{userId}", handler::subscribe)
        }
    }
}


fun main(args: Array<String>) {
    runApplication<Application>(*args)
}
