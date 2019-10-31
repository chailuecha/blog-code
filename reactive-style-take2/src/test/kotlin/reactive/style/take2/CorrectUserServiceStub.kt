package reactive.style.take2

import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import java.util.*
import java.util.concurrent.TimeUnit

class CorrectUserServiceStub : UserService{
    private val users = mutableMapOf<UUID, User>()

    override fun newUser(userId: UUID, userName: String, email: String): Mono<Void> {
       println("Construct mono for creating new user")
       return Mono.fromRunnable<Void> {
           println("Creating new user")
           users[userId] = User(userId, userName, email)
       }
    }

    override fun getUser(userId: UUID): Mono<User> {
        println("Construct mono for getting a user")
        return Mono.defer{
            println("Getting a user")
            users[userId]?.toMono() ?: Mono.empty()
        }
    }
}
