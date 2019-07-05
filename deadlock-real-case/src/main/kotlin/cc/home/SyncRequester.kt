package cc.home

import java.util.concurrent.atomic.AtomicReference

class SyncRequester(private val transport: AsyncTransport) {
    private val responseData = AtomicReference<String?>()
    private val lock = Object()

    fun request(subjectExpression: String, timeout: Long): String? {
        val subscription = transport.subscribe(subjectExpression, ::onUpdate)

        //wait for notification of response
        val until = nowMillis() + timeout
        synchronized(lock) {
            while (responseData.get() == null && nowMillis() < until) {
                //spurious wake up could happen before timeout
                val remainingTimeout = until - nowMillis()
                lock.wait(remainingTimeout)
            }

            //Sleep just to increase the chance in re-producing cc.home.deadlock
            //This sleep has nothing to do with business logic.
            Thread.sleep(200)

            //Should unsubscribe regardless of getting data or not.
            transport.unsubscribe(subscription)
        }

        return responseData.get()
    }

    private fun onUpdate(data: String) {
        if (responseData.get() == null) {
            responseData.set(data)
            synchronized(lock) {
                lock.notify() //wake up the request method
            }
        }
    }
}
