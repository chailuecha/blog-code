package reactive.style.take3_2

import mu.KotlinLogging
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

@Service
class WebRequestHandler {
    private val userMap = ConcurrentHashMap<String, UserDetail>()
    private val logger = KotlinLogging.logger { }

    fun createUser(request: ServerRequest): Mono<ServerResponse> {
        val userId = request.pathVariable("userId")
        return request
            .bodyToMono(UserDetail::class.java)
            .map { userDetail ->
                userMap[userId] = userDetail
                userDetail
            }
            .doOnNext {
                //simulate processing time to make the overhead more obvious
                TimeUnit.MILLISECONDS.sleep(100)
            }
            .then(ServerResponse.ok().bodyValue("User is created"))
    }

    fun subscribe(request: ServerRequest): Mono<ServerResponse> {
        val userId = request.pathVariable("userId")
        return request
            .bodyToMono(Subscription::class.java)
            .flatMap { subscription ->
                // This is fake implementation.
                // We just want to check if there is any subscription request
                // that come before create-user request

                when (userMap[userId]) {
                    null -> ServerResponse
                        .status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .bodyValue("Could not add subscription: ${subscription.newsLetterId}. " +
                                   "User $userId not exist")
                    else -> ServerResponse
                        .ok()
                        .bodyValue("Subscription is added")
                }
            }
            .doOnNext {
                //simulate processing time to make the overhead more obvious
                TimeUnit.MILLISECONDS.sleep(100)
            }
    }
}
