package com.cycloneboy.bigdata.communication.model

/**
 *
 * Create by  sl on 2019-12-03 16:15
 */
/**
 *
 * @param caller     第一个号码
 * @param callerName 第一个号码名称
 * @param callee     第二个手机号码
 * @param calleeName 第二个手机号码名称
 * @param dateTime   建立通话的时间
 * @param timestamp  建立通话的时间戳
 * @param duration   通话持续时间(秒)
 * @param flag       用于标记本次通话第一个字段号码是主叫还是被叫
 */
case class PhoneCallRecord(caller: String, callerName: String,
                           callee: String, calleeName: String,
                           dateTime: String, timestamp: Long,
                           duration: String, flag: String)
