package com.dwarf.core.convert;

import org.springframework.data.mapping.context.MappingContext;

import com.dwarf.core.mapping.ElasticsearchPersistentEntity;
import com.dwarf.core.mapping.ElasticsearchPersistentProperty;

public interface ElasticsearchConverter {

	/**
	 * Returns the underlying {@link org.springframework.data.mapping.context.MappingContext} used by the converter.
	 *
	 * @return never {@literal null}
	 */
	MappingContext<? extends ElasticsearchPersistentEntity<?>, ElasticsearchPersistentProperty> getMappingContext();

}
