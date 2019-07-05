package cc.home

import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class AsyncTransport(private val feed: Feed) {
    private val stopped = AtomicBoolean()
    private val dispatcherThread = Thread(::pollUpdateForSubscriptions, "UpdateDispatcherThread")

    // The code in real case is using Java Vector.
    // I preserve it here since it is part of concurrency characteristic in the code.
    private val subscriptions = Vector<Subscription>()

    private fun pollUpdateForSubscriptions() {
        while (!stopped.get()) {
            Thread.sleep(200) //poll interval
            pollUpdate()
        }
    }

    private fun pollUpdate() {
        feed.getUpdate()?.let { update ->
            //Block any subscriptionIds modification while iterating
            synchronized(subscriptions) {
                subscriptions.forEach { sub ->
                    if (update.forSubscription(sub)) {
                        sub.callback(update.data)
                    }
                }
            }
        }
    }

    fun subscribe(subjectExpression: String,
            callback: (data: String) -> Unit): Subscription {

        return Subscription(UUID.randomUUID().toString(), callback)
            .also { sub ->
                subscriptions.add(sub)
                feed.subscribe(sub.subId, subjectExpression)
            }
    }

    fun unsubscribe(subscription: Subscription) {
        subscriptions.remove(subscription)
        feed.unSubscribe(subscription.subId)
    }

    fun stop() {
        stopped.set(true)
    }

    fun start() {
        dispatcherThread.start()
    }
}
