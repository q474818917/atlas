package com.dwarf.core.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;

public class SimpleElasticsearchPersistentProperty extends
		AnnotationBasedPersistentProperty<ElasticsearchPersistentProperty> implements ElasticsearchPersistentProperty {

	private static final Set<Class<?>> SUPPORTED_ID_TYPES = new HashSet<Class<?>>();
	private static final Set<String> SUPPORTED_ID_PROPERTY_NAMES = new HashSet<String>();

	static {
		SUPPORTED_ID_TYPES.add(String.class);
		SUPPORTED_ID_PROPERTY_NAMES.add("id");
		SUPPORTED_ID_PROPERTY_NAMES.add("documentId");
	}

	public SimpleElasticsearchPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			PersistentEntity<?, ElasticsearchPersistentProperty> owner, SimpleTypeHolder simpleTypeHolder) {
		super(field, propertyDescriptor, owner, simpleTypeHolder);
	}

	@Override
	public String getFieldName() {
		return field.getName();
	}

	@Override
	public boolean isIdProperty() {
		return super.isIdProperty() || (field != null ? SUPPORTED_ID_PROPERTY_NAMES.contains(getFieldName()) : false);
	}

	@Override
	protected Association<ElasticsearchPersistentProperty> createAssociation() {
		return null;
	}
}