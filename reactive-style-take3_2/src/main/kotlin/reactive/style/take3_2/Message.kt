package reactive.style.take3_2

import java.util.UUID

data class Message(
        val userId: UUID,
        val userDetail: UserDetail?,
        val subscription: Subscription?
)

data class UserDetail(val email:String, val name:String)
data class Subscription(val newsLetterId: Int)
