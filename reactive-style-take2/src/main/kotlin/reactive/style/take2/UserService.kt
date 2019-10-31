package reactive.style.take2

import reactor.core.publisher.Mono
import java.util.*

interface UserService {
    fun newUser(userId: UUID, userName: String, email:String) : Mono<Void>
    fun getUser(userId: UUID) : Mono<User>
}
