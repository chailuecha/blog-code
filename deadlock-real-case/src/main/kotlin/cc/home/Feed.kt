package cc.home

import java.util.concurrent.ArrayBlockingQueue

interface Feed {
    fun subscribe(subId: String, subjectExpression: String)
    fun unSubscribe(subId: String)
    fun getUpdate(): Update?
}

class MockFeed : Feed {
    private val subscriptionIds = ArrayBlockingQueue<String>(1000)

    override fun subscribe(subId: String, subjectExpression: String) {
        //just mock feed, ignore reqExpression
        subscriptionIds.add(subId)
    }

    override fun unSubscribe(subId: String) {
        subscriptionIds.remove(subId)
    }

    override fun getUpdate(): Update? {
        val currentSubs = ArrayList<String>(subscriptionIds)
        return when (currentSubs.isEmpty()) {
            true -> null
            else -> {
                val randomIndex = (Math.random() * 10).toInt() % currentSubs.size
                Update(currentSubs[randomIndex], "Some Data")
            }
        }
    }

}
