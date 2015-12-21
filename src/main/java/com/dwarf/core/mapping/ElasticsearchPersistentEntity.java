package com.dwarf.core.mapping;

import org.springframework.data.mapping.PersistentEntity;

public interface ElasticsearchPersistentEntity<T> extends PersistentEntity<T, ElasticsearchPersistentProperty> {

	String getIndexName();

	String getIndexType();

	short getShards();

	short getReplicas();

	String getRefreshInterval();

	String getIndexStoreType();

	ElasticsearchPersistentProperty getVersionProperty();

	String getParentType();

	ElasticsearchPersistentProperty getParentIdProperty();

	String settingPath();
}
