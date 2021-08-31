package com.atomicvoid.orientdb.objectapi

import com.atomicvoid.orientdb.objectapi.annotations.*
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.record.impl.OEdgeDocument
import com.orientechnologies.orient.core.record.impl.OVertexDocument
import com.orientechnologies.orient.core.sql.executor.OResult
import org.slf4j.LoggerFactory
import org.reflections8.Reflections
import org.reflections8.scanners.SubTypesScanner
import org.reflections8.util.ClasspathHelper
import org.reflections8.util.ConfigurationBuilder
import org.reflections8.util.FilterBuilder
import java.lang.reflect.Field
import java.lang.reflect.ParameterizedType
import java.text.MessageFormat
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class OrientDBObjectApiImpl(override val session: ODatabaseSession) : OrientDBObjectApi {

    companion object {
        private val logger = LoggerFactory.getLogger(OrientDBObjectApiImpl::class.java.name)
        private val registeredClassForEntityMap: MutableMap<String?, Class<*>> = HashMap()
    }

    override fun registerEntityClasses(iPackageName: String?) {
        val iClassLoader = Thread.currentThread().contextClassLoader
        val urls = ClasspathHelper.forPackage(iPackageName, iClassLoader)
        val include = FilterBuilder().include(FilterBuilder.prefix(iPackageName))
        val classSet = Reflections(
            ConfigurationBuilder()
                .filterInputsBy(include)
                .addUrls(urls)
                .addClassLoader(iClassLoader)
                .setScanners(SubTypesScanner(false))
        )
            .getSubTypesOf(Any::class.java)
        classSet.stream().forEach { iClass: Class<*> -> registerEntityClass(iClass) }
    }

    override fun registerEntityClass(iClass: Class<*>) {
        if (iClass.isAnonymousClass) {
            return
        }
        session.activateOnCurrentThread()
        val entityClassName = getEntityClassName(iClass)
        registerEntityClass(entityClassName, iClass)
    }

    override fun registerEntityClass(
        entityClassName: String,
        iClass: Class<*>
    ) {
        if (iClass.isAnonymousClass) {
            return
        }
        session.activateOnCurrentThread()
        val oSchema = session.metadata.schema
        if (!oSchema.existsClass(entityClassName)) {
            val edgeClassAnnotation = iClass.getAnnotation(EdgeClass::class.java)
            if (edgeClassAnnotation != null) {
                oSchema.createClass(entityClassName, oSchema.getClass("E"))
            } else {
                oSchema.createClass(entityClassName, oSchema.getClass("V"))
            }
        }
        registeredClassForEntityMap[entityClassName] = iClass
    }

    override fun <RET> query(
        query: String,
        vararg objects: Any?
    ): List<RET> {
        session.activateOnCurrentThread()
        val result: MutableList<RET> = ArrayList()
        val resultSet = session.query(query, *objects)
        while (resultSet.hasNext()) {
            val oResult = resultSet.next()
            val toObject = toObject<RET>(oResult)
            toObject?.let { result.add(it) }
        }
        return result
    }

    override fun <RET> query(
        query: String,
        map: Map<String?, Any?>
    ): List<RET> {
        session.activateOnCurrentThread()
        val result: MutableList<RET> = ArrayList()
        val resultSet = session.query(query, map)
        while (resultSet.hasNext()) {
            val oResult = resultSet.next()
            toObject<RET>(oResult)?.let { result.add(it) }
        }
        return result
    }

    override fun <RET> command(
        query: String,
        vararg objects: Any?
    ): List<RET> {
        session.activateOnCurrentThread()
        val result: MutableList<RET> = ArrayList()
        val resultSet = session.command(query, *objects)
        while (resultSet.hasNext()) {
            val oResult = resultSet.next()
            toObject<RET>(oResult)?.let { result.add(it) }
        }
        return result
    }

    override fun <RET> command(
        query: String,
        map: Map<String?, Any?>
    ): List<RET> {
        session.activateOnCurrentThread()
        val result: MutableList<RET> = ArrayList()
        val resultSet = session.command(query, map)
        while (resultSet.hasNext()) {
            val oResult = resultSet.next()
            toObject<RET>(oResult)?.let { result.add(it) }
        }
        return result
    }

    private fun <RET> getEntityClassName(iClass: Class<RET>): String {
        val entityAnnotation = iClass.getAnnotation(Entity::class.java)
        return if (entityAnnotation != null) entityAnnotation.name else iClass.simpleName
    }

    override fun <RET> load(orid: ORID?): RET? {
        if (orid == null) {
            return null
        }
        session.activateOnCurrentThread()
        val document = session.load<ODocument>(orid) ?: return null
        return toObject(document)
    }

    override fun <RET> loadCollection(collection: Collection<ORID?>?): List<RET> {
        session.activateOnCurrentThread()
        val result: MutableList<RET> = ArrayList()
        collection?.forEach { orid: ORID? ->
            val record: RET? = load(orid)
            record?.let {
                result.add(record)
            }
        }
        return result
    }

    override fun <RET> findAll(iClass: Class<RET>): List<RET> {
        val entityAnnotation = iClass.getAnnotation(Entity::class.java)
        val query: String
        query = if (entityAnnotation != null) {
            "SELECT FROM " + entityAnnotation.name
        } else {
            "SELECT FROM " + iClass.simpleName
        }
        return query(query)
    }

    override fun <RET> saveAndGet(iContent: Any?): RET {
        session.activateOnCurrentThread()
        return toObject(session.save(toDocument(iContent)))!!
    }

    override fun save(iContent: Any?) {
        session.activateOnCurrentThread()
        session.save<ORecord>(toDocument(iContent))
    }

    override fun delete(iContent: Any?) {
        session.activateOnCurrentThread()
        getORID(iContent).ifPresent { orid: ORID? -> session.delete(orid) }
    }

    override fun beginTransaction() {
        session.begin()
    }

    override fun commitTransaction() {
        session.commit()
    }

    override fun rollbackTransaction() {
        session.rollback()
    }

    override fun close() {
        /*
                    String method = DBSessionProvider.Companion.getSessionMap().get(dbSession);
                    AtomicInteger counter = DBSessionProvider.Companion.getMethodMap().get(method);
                    if (counter != null) {
                        logger.info(String.format("Open/close counter for method %s: %d", method, counter.decrementAndGet()));
                    }
        */
        if (session.isClosed) {
            logger.error("dbSession closed prematurely")
        }
        //            logger.info("Closing " + dbSession);
        try {
            session.close()
        } catch (exception: Exception) {
            logger.error("Could not return session", exception)
            throw exception
        }
    }

    override fun count(iClass: Class<*>): Int {
        val entityName: String
        val entityAnnotation: Entity? = iClass.getAnnotation(Entity::class.java)
        if (entityAnnotation != null) {
            entityName = entityAnnotation.name
        } else {
            entityName = iClass.simpleName
        }
        session.query("SELECT COUNT(*) AS count FROM $entityName").use { oResult ->
            return if (oResult.hasNext()) {
                oResult.next().getProperty("count")
            } else 0
        }
    }

    override fun getORID(iContent: Any?): Optional<ORID> {
        session.activateOnCurrentThread()
        var orid: ORID? = null
        for (field in iContent!!.javaClass.declaredFields) {
            val oidAnnotation: OId? = field.getAnnotation(OId::class.java)
            if (oidAnnotation != null) {
                try {
                    field.isAccessible = true
                    val oridObj = field[iContent]
                    if (oridObj != null) {
                        orid = oridObj as ORID
                    }
                    break
                } catch (e: IllegalAccessException) {
                    throw RuntimeException(e)
                }
            }
        }
        return Optional.ofNullable(orid)
    }

    private fun toDocument(iContent: Any?): ORecord? {
        var isEdgeClass = false
        val iClass: Class<*> = iContent!!.javaClass
        val edgeClassAnnotation: EdgeClass? = iClass.getAnnotation(EdgeClass::class.java)
        if (edgeClassAnnotation != null) {
            isEdgeClass = true
        }
        val entityName: String
        val entityAnnotation = iClass.getAnnotation(Entity::class.java)
        if (entityAnnotation != null) {
            entityName = entityAnnotation.name
        } else {
            entityName = iClass.simpleName
        }
        val triple = collectFields(iClass, iContent)
        val orid: ORID? = triple.first
        val outEdgeFields: MutableSet<Field> = triple.second
        val valueFields: MutableSet<Field?> = triple.third
        if (!isEdgeClass) {
            val oVertex = if (orid != null) session.load(orid) else session.newVertex(entityName)
            loadFieldProperties(valueFields, iContent, oVertex)
            createOutEdges(outEdgeFields, entityName, iContent, oVertex)
            return oVertex
        }
        throw IllegalArgumentException("EdgeClass not yet supported")
    }

    private fun loadFieldProperties(
        valueFields: MutableSet<Field?>,
        iContent: Any?,
        oVertex: OVertex
    ) {
        valueFields.forEach { field: Field? ->
            val oVersionAnnotation: OVersion? = field!!.getAnnotation(OVersion::class.java)
            if (oVersionAnnotation != null) return@forEach
            val propertyAnnotation = field.getAnnotation(Property::class.java)
            val fieldName = propertyAnnotation?.value ?: field.name
            try {
                val mappedValue = mapToDocumentValue(field.type, getGenericClass(field), field[iContent])
                val currentValue: Any? = oVertex.getProperty(fieldName)
                if (!Objects.equals(currentValue, mappedValue)) {
                    oVertex.setProperty(fieldName, mappedValue)
                }
            } catch (e: IllegalAccessException) {
                throw RuntimeException(e)
            }
        }
    }

    private fun createOutEdges(
        outEdgeFields: MutableSet<Field>,
        entityName: String,
        iContent: Any?,
        oVertex: OVertex
    ) {
        outEdgeFields.forEach { field: Field ->
            val outAnnotation = field.getAnnotation(Out::class.java)
            val edgeLabel = outAnnotation.edgeLabel.ifEmpty { entityName + "To" + field.javaClass.simpleName }
            val toObject: Any? = field.get(iContent)
            if (toObject is Collection<*>) {
                toObject.forEach { entry: Any? ->
                    val toVertex: OVertexDocument = getToVertex(entry!!, outAnnotation)
                    createEdgeIfNotExists(oVertex, toVertex, edgeLabel)
                }
            } else if (toObject != null) {
                val toVertex: OVertexDocument = getToVertex(toObject, outAnnotation)
                createEdgeIfNotExists(oVertex, toVertex, edgeLabel)
            }
        }
    }

    private fun collectFields(
        iClass: Class<*>,
        iContent: Any?
    ): Triple<ORID?, MutableSet<Field>, MutableSet<Field?>> {
        var orid: ORID? = null
        val outEdgeFields: MutableSet<Field> = hashSetOf()
        val valueFields: MutableSet<Field?> = hashSetOf()
        for (field in iClass.declaredFields) {
            field.isAccessible = true
            val inAnnotation = field.getAnnotation(In::class.java)
            if (inAnnotation != null) {
                continue
            }
            val outAnnotation = field.getAnnotation(Out::class.java)
            if (outAnnotation != null) {
                outEdgeFields.add(field)
                continue
            }
            val oidAnnotation: OId? = field.getAnnotation(OId::class.java)
            if (oidAnnotation != null) {
                try {
                    val idObj = field[iContent]
                    if (idObj != null) {
                        orid = idObj as ORID
                    }
                    continue
                } catch (e: IllegalAccessException) {
                    throw RuntimeException(e)
                }
            }
            valueFields.add(field)
        }
        return Triple(orid, outEdgeFields, valueFields)
    }


    private fun getToVertex(toObject: Any, outAnnotation: Out): OVertexDocument {
        return getORID(toObject).map {
            if (outAnnotation.cascadeSaveUpdate) {
                return@map session.save(toDocument(toObject)) as OVertexDocument
            }
            return@map toDocument(toObject) as OVertexDocument
        }.orElseGet {
            if (outAnnotation.cascadeSaveNew) {
                return@orElseGet session.save(toDocument(toObject)) as OVertexDocument
            }
            return@orElseGet toDocument(toObject) as OVertexDocument
        }
    }

    private fun createEdgeIfNotExists(
        oVertex: OVertex,
        oVertexDoc: OVertexDocument,
        edgeLabel: String
    ) {
        val edges = oVertex.getEdges(ODirection.OUT, edgeLabel)
        val edgeExists = AtomicBoolean()
        edges.forEach { oEdge: OEdge ->
            if (oEdge.to == oVertexDoc) {
                edgeExists.set(true)
            }
        }
        if (!edgeExists.get()) {
            session.newEdge(oVertex, oVertexDoc, edgeLabel)
        }
    }

    fun <RET> toObject(iContent: Any): RET? {
        if (iContent is ORID) {
            val oVertexDocument = session.load<OVertexDocument>(iContent)
            return toObject(oVertexDocument)
        } else if (iContent is OVertexDocument) {
            return toObject(iContent)
        } else if (iContent is OResult) {
            val oResult = iContent
            if (oResult.isVertex) {
                val oVertexDocument = oResult.vertex.get() as OVertexDocument
                return toObject(oVertexDocument)
            }
            throw IllegalArgumentException("$oResult type not supported")
        } else if (iContent is ORidBag) {
            val resultList: MutableList<Any> = mutableListOf()
            iContent.forEach { it ->
                val oDocument: ODocument? = session.load(it.identity)
                oDocument?.let {
                    when (oDocument) {
                        is OEdgeDocument -> {
                            val outOVertex: OVertexDocument = oDocument.getProperty("in") // The In side is connected to this vertex when reading
                            toObject<RET>(outOVertex)?.let { resultList.add(it) }
                        }
                        is OVertexDocument -> {
                            toObject<RET>(it)?.let { resultList.add(it) }
                        }
                    }

                }
            }
            return resultList as RET
        } else if (iContent is OEdgeDocument) {
            val outOVertex: OVertexDocument = iContent.getProperty("in") // The In side is connected to this vertex when reading
            return toObject(outOVertex)
        }

        throw IllegalArgumentException("" + iContent.javaClass + " not supported")
    }

    fun <RET> toObject(oDocument: OVertexDocument): RET? {
        val entityName = oDocument.className
        val iClass = registeredClassForEntityMap[entityName]
            ?: throw RuntimeException(
                MessageFormat.format(
                    "No registered class found for entity {0}, please use registerEntityClass() to register them first.",
                    entityName
                )
            )
        return try {
            val resultObject = iClass.newInstance() as RET
            val edgeClassAnnotation: EdgeClass? = iClass.getAnnotation(EdgeClass::class.java)
            require(edgeClassAnnotation == null) { "Edge class not yet supported" }
            for (field in iClass.declaredFields) {
                field.isAccessible = true
                val inAnnotation: In? = field.getAnnotation(In::class.java)
                if (inAnnotation != null) {
                    val edgeLabel = inAnnotation.edgeLabel.ifEmpty { entityName + "To" + field.javaClass.simpleName }
                    val `object` = oDocument.getProperty<Any>("in_$edgeLabel")
                    field[resultObject] = toObject(`object`)
                    continue
                }
                val outAnnotation: Out? = field.getAnnotation(Out::class.java)
                if (outAnnotation != null) {
                    val edgeLabel = outAnnotation.edgeLabel.ifEmpty { entityName + "To" + field.javaClass.simpleName }
                    val outElement: Any = oDocument.getProperty<Any?>("out_$edgeLabel") ?: continue
                    val toObject: Any? = toObject(outElement)
                    if (toObject is Collection<*> && !Collection::class.java.isAssignableFrom(field.type)) {
                        toObject.firstNotNullOf { // We may get an RidBag while we linked a single entity
                            field[resultObject] = it
                        }
                    } else {
                        field[resultObject] = toObject
                    }
                    continue
                }
                val oidAnnotation: OId? = field.getAnnotation(OId::class.java)
                if (oidAnnotation != null) {
                    field[resultObject] = oDocument.identity
                    continue
                }
                val oVersionAnnotation: OVersion? = field.getAnnotation(OVersion::class.java)
                if (oVersionAnnotation != null) {
                    field[resultObject] = oDocument.version
                    continue
                }
                val propertyAnnotation: Property? = field.getAnnotation(Property::class.java)
                val fieldName = if (propertyAnnotation != null && propertyAnnotation.value.isNotEmpty()) propertyAnnotation.value else field.name
                val value = oDocument.getProperty<Any>(fieldName)
                val mapToObjectValue = mapToObjectValue(field.type, getGenericClass(field), value)
                if (mapToObjectValue != null || !field.type.isPrimitive) {
                    field[resultObject] = mapToObjectValue
                }
            }
            return resultObject
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    private fun mapToObjectValue(fieldClass: Class<*>, genericClass: Class<*>?, value: Any?): Any? {
        when {
            fieldClass.isEnum && value is String -> {
                return getEnumValue(fieldClass, value)
            }
            Set::class.java.isAssignableFrom(fieldClass) -> {
                val setValue = hashSetOf<Any>();
                (value as Set<Any>).forEach {
                    val element: Any? = mapToObjectValue(genericClass!!, null, it)
                    if (element != null) {
                        setValue.add(element)
                    }
                }
                return setValue
            }
            Collection::class.java.isAssignableFrom(fieldClass) -> {
                val listValue = mutableListOf<Any>();
                (value as Collection<Any>).forEach {
                    val element: Any? = mapToObjectValue(genericClass!!, null, it)
                    if (element != null) {
                        listValue.add(element)
                    }
                }
                return listValue
            }
        }
        return value
    }

    private fun mapToDocumentValue(fieldClass: Class<*>, genericClass: Class<*>?, value: Any?): Any? {
        when {
            fieldClass.isEnum && value != null -> {
                return getEnumName(fieldClass, value as Enum<*>)
            }
            Set::class.java.isAssignableFrom(fieldClass) -> {
                val setValue = hashSetOf<Any>();
                (value as Set<Any>).forEach {
                    val element: Any? = mapToDocumentValue(genericClass!!, null, it)
                    if (element != null) {
                        setValue.add(element)
                    }
                }
                return setValue
            }
            Collection::class.java.isAssignableFrom(fieldClass) -> {
                val listValue = mutableListOf<Any>();
                (value as Collection<Any>).forEach {
                    val element: Any? = mapToDocumentValue(genericClass!!, null, it)
                    if (element != null) {
                        listValue.add(element)
                    }
                }
                return listValue
            }
        }
        return value
    }


    fun getEnumValue(enumClass: Class<*>, enumValue: String): Any? {
        val enumClz = enumClass.enumConstants as Array<Enum<*>>
        return enumClz.firstOrNull { it.name == enumValue }
    }

    fun getEnumName(enumClass: Class<*>, enumValue: Enum<*>): Any? {
        val enumClz = enumClass.enumConstants as Array<Enum<*>>
        return enumClz.firstOrNull { it == enumValue }?.name
    }

    private fun getGenericClass(field: Field): Class<*>? {
        var genericClass: Class<*>? = null
        if (field.genericType is ParameterizedType) {
            val actualTypeArguments = (field.genericType as ParameterizedType).actualTypeArguments
            if (actualTypeArguments.isNotEmpty()) {
                genericClass = actualTypeArguments[0] as Class<*>
            }
        }
        return genericClass
    }

}