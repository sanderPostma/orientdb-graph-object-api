package com.atomicvoid.orientdb.objectapi.annotations

import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy

@Retention(RetentionPolicy.RUNTIME)
@Target(AnnotationTarget.FIELD, AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY_GETTER, AnnotationTarget.PROPERTY_SETTER, AnnotationTarget.ANNOTATION_CLASS)
annotation class Out(val edgeLabel: String = "", val cascadeSaveNew: Boolean = true, val cascadeSaveUpdate: Boolean = false)