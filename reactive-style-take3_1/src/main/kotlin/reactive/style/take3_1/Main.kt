package reactive.style.take3_1

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

data class Message(val guid: Int)

fun main() {
    val messages = Flux.just(
        Message(guid = 1),
        Message(guid = 2),
        Message(guid = 3)
    )

    /* =========================
    Uncomment flatMap(), flatMapSequential() or concatMap() to see the difference
    ============================ */
    messages
        /* =============  flatMap :
            - Call processInParallel() for each message to be asynchronously executed.
            - Since it is asynchronous, flatmap doesn't wait for current message to be actually executed
            before submitting next message to processInParallel()
            - Output from processInParallel() which will be pass on to the next step can be in different order than
            the original source messages.
         */
        .flatMap { msg -> processInParallel(msg) }

        /* ============= flatMapSequential :
            - Call processInParallel() for each message to be asynchronously executed.
            - Since it is asynchronous, flatmap doesn't wait for current message to be actually executed
            before submitting next message to processInParallel()
            - Even though each message could result in output in different order,
            flatMapSequential will arrange the output to be in the same order of the original source messages.
         */
        //.flatMapSequential(::processInParallel)

        /* =============  concatMap :
            - Call processInParallel() for each message to be asynchronously executed but wait for the method to
            finish processing the message before submitting the message. This is practically means the messages are
            sequentially executed.
            - Since each messages are sequentially executed, the output will be in the same order
            of the original source messages.
         */
        //.concatMap(::processInParallel)

        .map { println("Thread(${Thread.currentThread().name}) - Do next step $it") }
        .blockLast()
}

fun processInParallel(msg: Message): Mono<Message> {
    return Mono
        .fromRunnable<Void> {
            println("Thread(${Thread.currentThread().name}) - Start processing $msg")
            Thread.sleep((Math.random() * 5000).toLong())
            println("Thread(${Thread.currentThread().name}) - End processing $msg")
        }
        .subscribeOn(Schedulers.parallel()) // Process task in thread pool
        .thenReturn(msg)
}
