package reactive.style.take2

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import reactor.test.StepVerifier
import java.util.*

class UserServiceStubTest {
    private val userService = IncorrectUserServiceStub()
//    private val userService = CorrectUserServiceStub()

    @Test
    fun `create new user and query the user back by userId`(){
        val userId = UUID.randomUUID()
        val resultMono = userService
            .newUser(userId, "userA", "userA@gmail.com")
            .then(userService.getUser(userId))


        StepVerifier
            .create( resultMono )
            .assertNext { user ->
                assertEquals("userA", user.name)
                assertEquals("userA@gmail.com", user.email)
            }
            .verifyComplete()
    }
}
