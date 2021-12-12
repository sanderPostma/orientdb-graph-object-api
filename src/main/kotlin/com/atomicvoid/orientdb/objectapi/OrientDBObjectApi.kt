package com.atomicvoid.orientdb.objectapi

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.id.ORID
import java.util.*

interface OrientDBObjectApi : AutoCloseable {
    fun registerEntityClasses(iPackageName: String?)
    fun registerEntityClass(iClass: Class<*>)
    fun registerEntityClass(
        entityName: String,
        iClass: Class<*>
    )

    fun <RET> query(
        query: String,
        vararg objects: Any?
    ): List<RET>

    fun <RET> query(
        query: String,
        map: Map<String?, Any?>
    ): List<RET>

    fun <RET> command(
        query: String,
        vararg objects: Any?
    ): List<RET>

    fun <RET> command(
        query: String,
        map: Map<String?, Any?>
    ): List<RET>

    fun <RET> load(orid: ORID?): RET?
    fun <RET> load(orid: ORID?, iClass: Class<RET>): RET?
    fun <RET> lock(orid: ORID?): RET?
    fun unlock(orid: ORID?)
    fun <RET> loadCollection(collection: Collection<ORID?>?): List<RET>
    fun <RET> loadCollection(collection: Collection<ORID?>?, iClass: Class<RET>): List<RET>
    fun <RET> findAll(iClass: Class<RET>): List<RET>
    fun <RET> saveAndGet(iContent: Any?): RET
    fun save(iContent: Any?): ORID
    fun delete(iContent: Any?)
    fun getORID(iContent: Any?): Optional<ORID>
    fun beginTransaction()
    fun commitTransaction()
    fun rollbackTransaction()
    val session: ODatabaseSession
    fun count(iClass: Class<*>): Int
}
