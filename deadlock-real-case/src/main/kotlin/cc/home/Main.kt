package cc.home

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

data class Subscription(val subId: String, val callback: (data: String) -> Unit)

data class Update(val subId: String, val data: String) {
    fun forSubscription(sub: Subscription): Boolean = sub.subId == subId
}

fun main() {
    //testAsyncSubscription()
    deadlockWithSyncSyncRequester()
}

fun testAsyncSubscription() {
    val feed = MockFeed()
    val transport = AsyncTransport(feed)
    transport.start()

    val subscription = transport.subscribe("subject:news") { updateData ->
        println("New Update: $updateData")
    }

    //wait to get update for 10 seconds
    Thread.sleep(10 * 1000)
    println("UnSubscribing")
    transport.unsubscribe(subscription)

    transport.stop()
}

fun deadlockWithSyncSyncRequester() {

    val feed = MockFeed()
    val transport = AsyncTransport(feed)

    transport.start()

    val executor = Executors.newFixedThreadPool(50)

    for (i in 1..1000) {
        val randomTimeout = (Math.random() * 10000).toLong()

        executor.submit {
            SyncRequester(transport)
                .request("syn-req-$i", randomTimeout)
                .let { println("Response: $it") }
        }
    }

    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.HOURS)
    transport.stop()
}


fun nowMillis() = System.currentTimeMillis()
