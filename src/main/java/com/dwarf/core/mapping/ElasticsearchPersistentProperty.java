package com.dwarf.core.mapping;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mapping.PersistentProperty;

public interface ElasticsearchPersistentProperty extends PersistentProperty<ElasticsearchPersistentProperty> {

	String getFieldName();

	public enum PropertyToFieldNameConverter implements Converter<ElasticsearchPersistentProperty, String> {

		INSTANCE;

		public String convert(ElasticsearchPersistentProperty source) {
			return source.getFieldName();
		}
	}
}
