package com.atomicvoid.orientdb.objectapi.annotations

import com.orientechnologies.orient.core.metadata.schema.OClass

annotation class Indexed(val name: String, val indexType: OClass.INDEX_TYPE) {

}
