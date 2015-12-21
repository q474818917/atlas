package com.dwarf.core;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import com.dwarf.exception.ElasticsearchException;

public abstract class AbstractResultMapper implements ResultsMapper {

	private EntityMapper entityMapper;

	public AbstractResultMapper(EntityMapper entityMapper) {
		this.entityMapper = entityMapper;
	}

	public <T> T mapEntity(String source, Class<T> clazz) {
		if (StringUtils.isBlank(source)) {
			return null;
		}
		try {
			return entityMapper.mapToObject(source, clazz);
		} catch (IOException e) {
			throw new ElasticsearchException("failed to map source [ " + source + "] to class " + clazz.getSimpleName(), e);
		}
	}

	@Override
	public EntityMapper getEntityMapper() {
		return this.entityMapper;
	}
}
