const { checkFieldPropertiesChanged } = require('./common');

const getAddCollectionScript = (app, dbVersion) => collection => {
	const _ = app.require('lodash');
	const { getEntityName } = require('../../utils/general')(_);
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const ddlProvider = require('../../ddlProvider')(null, null, app);

	const schemaName = collection.compMod.keyspaceName;
	const schemaData = { schemaName, dbVersion };
	const jsonSchema = { ...collection, ...(_.omit(collection?.role, 'properties') || {}) };
	const columnDefinitions = _.toPairs(jsonSchema.properties).map(([name, column]) =>
		createColumnDefinitionBySchema({
			name,
			jsonSchema: column,
			parentJsonSchema: jsonSchema,
			ddlProvider,
			schemaData,
		}),
	);
	const checkConstraints = (jsonSchema.chkConstr || []).map(check =>
		ddlProvider.createCheckConstraint(ddlProvider.hydrateCheckConstraint(check)),
	);
	const tableData = {
		name: getEntityName(jsonSchema),
		columns: columnDefinitions.map(data => ddlProvider.convertColumnDefinition(data)),
		checkConstraints: checkConstraints,
		foreignKeyConstraints: [],
		schemaData,
		columnDefinitions,
	};
	const hydratedTable = ddlProvider.hydrateTable({ tableData, entityData: [jsonSchema], jsonSchema });

	return ddlProvider.createTable(hydratedTable, jsonSchema.isActivated);
};

const getDeleteCollectionScript = app => collection => {
	const _ = app.require('lodash');
	const { getEntityName } = require('../../utils/general')(_);
	const { getNamePrefixedWithSchemaName } = require('../general')({ _ });

	const jsonData = { ...collection, ...(_.omit(collection?.role, 'properties') || {}) };
	const tableName = getEntityName(jsonData);
	const schemaName = collection.compMod.keyspaceName;
	const fullName = getNamePrefixedWithSchemaName(tableName, schemaName);

	return `DROP TABLE ${fullName};`;
};

const getAddColumnScript = (app, dbVersion) => collection => {
	const _ = app.require('lodash');
	const { getEntityName } = require('../../utils/general')(_);
	const { getNamePrefixedWithSchemaName } = require('../general')({ _ });
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const ddlProvider = require('../../ddlProvider')(null, null, app);

	const collectionSchema = { ...collection, ...(_.omit(collection?.role, 'properties') || {}) };
	const tableName = getEntityName(collectionSchema);
	const schemaName = collectionSchema.compMod?.keyspaceName;
	const fullName = getNamePrefixedWithSchemaName(tableName, schemaName);
	const schemaData = { schemaName, dbVersion };

	return _.toPairs(collection.properties)
		.filter(([name, jsonSchema]) => !jsonSchema.compMod)
		.map(([name, jsonSchema]) =>
			createColumnDefinitionBySchema({
				name,
				jsonSchema,
				parentJsonSchema: collectionSchema,
				ddlProvider,
				schemaData,
			}),
		)
		.map(data => ddlProvider.convertColumnDefinition(data))
		.map(script => `ALTER TABLE ${fullName} ADD (${script});`);
};

const getDeleteColumnScript = app => collection => {
	const _ = app.require('lodash');
	const { getEntityName } = require('../../utils/general')(_);
	const { getNamePrefixedWithSchemaName, wrapInQuotes } = require('../general')({ _ });
	const collectionSchema = { ...collection, ...(_.omit(collection?.role, 'properties') || {}) };
	const tableName = getEntityName(collectionSchema);
	const schemaName = collectionSchema.compMod?.keyspaceName;
	const fullName = getNamePrefixedWithSchemaName(tableName, schemaName);

	return _.toPairs(collection.properties)
		.filter(([name, jsonSchema]) => !jsonSchema.compMod)
		.map(([name]) => `ALTER TABLE ${fullName} DROP COLUMN ${wrapInQuotes(name)};`);
};

const getModifyColumnScript = app => collection => {
	const _ = app.require('lodash');
	const { getEntityName } = require('../../utils/general')(_);
	const { getNamePrefixedWithSchemaName, wrapInQuotes } = require('../general')({ _ });

	const collectionSchema = { ...collection, ...(_.omit(collection?.role, 'properties') || {}) };
	const tableName = getEntityName(collectionSchema);
	const schemaName = collectionSchema.compMod?.keyspaceName;
	const fullName = getNamePrefixedWithSchemaName(tableName, schemaName);

	const renameColumnScripts = _.values(collection.properties)
		.filter(jsonSchema => checkFieldPropertiesChanged(jsonSchema.compMod, ['name']))
		.map(
			jsonSchema =>
				`ALTER TABLE ${fullName} RENAME COLUMN ${wrapInQuotes(
					jsonSchema.compMod.oldField.name,
				)} TO ${wrapInQuotes(jsonSchema.compMod.newField.name)};`,
		);

	const changeTypeScripts = _.toPairs(collection.properties)
		.filter(([name, jsonSchema]) => checkFieldPropertiesChanged(jsonSchema.compMod, ['type', 'mode']))
		.map(
			([name, jsonSchema]) =>
				`ALTER TABLE ${fullName} MODIFY (${wrapInQuotes(name)} ${
					_.toUpper(jsonSchema.compMod.newField.mode || jsonSchema.compMod.newField.type)
			});`,
		);

	return [...renameColumnScripts, ...changeTypeScripts];
};

module.exports = {
	getAddCollectionScript,
	getDeleteCollectionScript,
	getAddColumnScript,
	getDeleteColumnScript,
	getModifyColumnScript,
};
