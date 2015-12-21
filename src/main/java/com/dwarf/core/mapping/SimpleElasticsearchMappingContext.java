package com.dwarf.core.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import com.dwarf.core.mapping.ElasticsearchPersistentProperty;
import com.dwarf.core.mapping.SimpleElasticsearchPersistentEntity;
import com.dwarf.core.mapping.SimpleElasticsearchPersistentProperty;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

public class SimpleElasticsearchMappingContext
		extends AbstractMappingContext<SimpleElasticsearchPersistentEntity<?>, ElasticsearchPersistentProperty>
		implements ApplicationContextAware {

	private ApplicationContext context;

	@Override
	protected <T> SimpleElasticsearchPersistentEntity<?> createPersistentEntity(TypeInformation<T> typeInformation) {
		final SimpleElasticsearchPersistentEntity<T> persistentEntity = new SimpleElasticsearchPersistentEntity<T>(
				typeInformation);
		if (context != null) {
			persistentEntity.setApplicationContext(context);
		}
		return persistentEntity;
	}

	@Override
	protected ElasticsearchPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
			SimpleElasticsearchPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		return new SimpleElasticsearchPersistentProperty(field, descriptor, owner, simpleTypeHolder);
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		this.context = context;
	}
}
