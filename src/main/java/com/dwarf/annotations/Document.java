package com.dwarf.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.Persistent;

@Persistent
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Document {

	String indexName();

	String type() default "";

	short shards() default 5;

	short replicas() default 1;

	String refreshInterval() default "1s";

	String indexStoreType() default "fs";
}
