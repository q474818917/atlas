package com.dwarf.core.mapping;

import static org.springframework.util.StringUtils.hasText;

import java.util.Locale;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import com.dwarf.annotations.Document;
import com.dwarf.annotations.Parent;
import com.dwarf.annotations.Setting;
import com.dwarf.core.mapping.ElasticsearchPersistentEntity;
import com.dwarf.core.mapping.ElasticsearchPersistentProperty;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

public class SimpleElasticsearchPersistentEntity<T> extends BasicPersistentEntity<T, ElasticsearchPersistentProperty>
		implements ElasticsearchPersistentEntity<T>, ApplicationContextAware {

	private final StandardEvaluationContext context;
	private final SpelExpressionParser parser;

	private String indexName;
	private String indexType;
	private short shards;
	private short replicas;
	private String refreshInterval;
	private String indexStoreType;
	private String parentType;
	private ElasticsearchPersistentProperty parentIdProperty;
	private String settingPath;

	public SimpleElasticsearchPersistentEntity(TypeInformation<T> typeInformation) {
		super(typeInformation);
		this.context = new StandardEvaluationContext();
		this.parser = new SpelExpressionParser();

		Class<T> clazz = typeInformation.getType();
		if (clazz.isAnnotationPresent(Document.class)) {
			Document document = clazz.getAnnotation(Document.class);
			Assert.hasText(document.indexName(),
					" Unknown indexName. Make sure the indexName is defined. e.g @Document(indexName=\"foo\")");
			this.indexName = typeInformation.getType().getAnnotation(Document.class).indexName();
			this.indexType = hasText(document.type()) ? document.type()
					: clazz.getSimpleName().toLowerCase(Locale.ENGLISH);
			this.shards = typeInformation.getType().getAnnotation(Document.class).shards();
			this.replicas = typeInformation.getType().getAnnotation(Document.class).replicas();
			this.refreshInterval = typeInformation.getType().getAnnotation(Document.class).refreshInterval();
			this.indexStoreType = typeInformation.getType().getAnnotation(Document.class).indexStoreType();
		}
		if (clazz.isAnnotationPresent(Setting.class)) {
			this.settingPath = typeInformation.getType().getAnnotation(Setting.class).settingPath();
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		context.addPropertyAccessor(new BeanFactoryAccessor());
		context.setBeanResolver(new BeanFactoryResolver(applicationContext));
		context.setRootObject(applicationContext);
	}

	@Override
	public String getIndexName() {
		Expression expression = parser.parseExpression(indexName, ParserContext.TEMPLATE_EXPRESSION);
		return expression.getValue(context, String.class);
	}

	@Override
	public String getIndexType() {
		Expression expression = parser.parseExpression(indexType, ParserContext.TEMPLATE_EXPRESSION);
		return expression.getValue(context, String.class);
	}

	@Override
	public String getIndexStoreType() {
		return indexStoreType;
	}

	@Override
	public short getShards() {
		return shards;
	}

	@Override
	public short getReplicas() {
		return replicas;
	}

	@Override
	public String getRefreshInterval() {
		return refreshInterval;
	}

	@Override
	public String getParentType() {
		return parentType;
	}

	@Override
	public ElasticsearchPersistentProperty getParentIdProperty() {
		return parentIdProperty;
	}

	@Override
	public String settingPath() {
		return settingPath;
	}

	@Override
	public void addPersistentProperty(ElasticsearchPersistentProperty property) {
		super.addPersistentProperty(property);

		if (property.getField() != null) {
			Parent parent = property.getField().getAnnotation(Parent.class);
			if (parent != null) {
				Assert.isNull(this.parentIdProperty, "Only one field can hold a @Parent annotation");
				Assert.isNull(this.parentType, "Only one field can hold a @Parent annotation");
				Assert.isTrue(property.getType() == String.class, "Parent ID property should be String");
				this.parentIdProperty = property;
				this.parentType = parent.type();
			}
		}

		if (property.isVersionProperty()) {
			Assert.isTrue(property.getType() == Long.class, "Version property should be Long");
		}
	}
}