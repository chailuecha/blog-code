package cc.home.msg.dedup

import cc.home.msg.dedup.consumer.ManualCommitConsumer
import org.springframework.boot.WebApplicationType
import org.springframework.boot.runApplication

fun main(args: Array<String>) {
    //====================
    // ManualCommitConsumer
    //====================
    runApplication<ManualCommitConsumer>(*args) {
        webApplicationType = WebApplicationType.NONE
    }


    //====================
    // AutoCommitConsumer
    //====================
//    runApplication<AutoCommitConsumer>(*args) {
//        webApplicationType = WebApplicationType.NONE
//    }
}
