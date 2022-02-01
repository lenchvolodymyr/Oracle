const { checkFieldPropertiesChanged } = require('./common');
const templates = require('../../configs/templates');


const getCreateUdtScript = (app, dbVersion) => jsonSchema => {
	const _ = app.require('lodash');
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const ddlProvider = require('../../ddlProvider')(null, null, app);

	const schemaData = { dbVersion };

	const columnDefinitions = _.toPairs(jsonSchema.properties || {}).map(([name, column]) =>
		createColumnDefinitionBySchema({
			name,
			jsonSchema: column,
			parentJsonSchema: jsonSchema,
			ddlProvider,
			schemaData,
		}),
	);

	const updatedUdt = createColumnDefinitionBySchema({
		name: jsonSchema.code || jsonSchema.name,
		jsonSchema: jsonSchema,
		parentJsonSchema: { required: [] },
		ddlProvider,
		schemaData,
	});

	const udt = { ...updatedUdt, properties: columnDefinitions };
	return ddlProvider.createUdt(udt);
};

const getDeleteUdtScript = app => udt => {
	const _ = app.require('lodash');
	const { wrapInQuotes } = require('../general')({ _ });

	return `DROP TYPE ${wrapInQuotes(udt.code || udt.name)};`;
};

const getAddColumnToTypeScript = (app, dbVersion) => udt => {
	const _ = app.require('lodash');
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const { wrapInQuotes } = require('../general')({ _ });
	const ddlProvider = require('../../ddlProvider')(null, null, app);

	const fullName = wrapInQuotes(udt.code || udt.name);
	const schemaData = { dbVersion };

	return _.toPairs(udt.properties)
		.filter(([name, jsonSchema]) => !jsonSchema.compMod)
		.map(([name, jsonSchema]) =>
			createColumnDefinitionBySchema({
				name,
				jsonSchema,
				parentJsonSchema: { required: [] },
				ddlProvider,
				schemaData,
			}),
		)
		.map(data => ddlProvider.convertColumnDefinition(data, templates.objectTypeColumnDefinition))
		.map(script => `ALTER TYPE ${fullName} ADD ATTRIBUTE ${script};`);
};

const getDeleteColumnFromTypeScript = app => udt => {
	const _ = app.require('lodash');
	const { wrapInQuotes } = require('../general')({ _ });

	const fullName = wrapInQuotes(udt.code || udt.name);

	return _.toPairs(udt.properties)
		.filter(([name, jsonSchema]) => !jsonSchema.compMod)
		.map(([name]) => `ALTER TYPE ${fullName} DROP ATTRIBUTE ${wrapInQuotes(name)};`);
};

const getModifyColumnOfTypeScript = app => udt => {
	const _ = app.require('lodash');
	const { wrapInQuotes } = require('../general')({ _ });

	const fullName = wrapInQuotes(udt.code || udt.name);

	const renameColumnScripts = _.values(udt.properties)
		.filter(jsonSchema => checkFieldPropertiesChanged(jsonSchema.compMod, ['name']))
		.map(
			jsonSchema =>
				`ALTER TYPE ${fullName} DROP ATTRIBUTE ${wrapInQuotes(jsonSchema.compMod.oldField.name)};
				 ALTER TYPE ${fullName} ADD ATTRIBUTE ${wrapInQuotes(jsonSchema.compMod.newField.name)};`,
		);

	const changeTypeScripts = _.toPairs(udt.properties)
		.filter(([name, jsonSchema]) => checkFieldPropertiesChanged(jsonSchema.compMod, ['type', 'mode']))
		.map(
			([name, jsonSchema]) =>
				`ALTER TYPE ${fullName} MODIFY ATTRIBUTE ${wrapInQuotes(name)} ${
					_.toUpper(jsonSchema.compMod.newField.mode || jsonSchema.compMod.newField.type)
				};`,
		);

	return [...renameColumnScripts, ...changeTypeScripts];
};

module.exports = {
	getCreateUdtScript,
	getDeleteUdtScript,
	getAddColumnToTypeScript,
	getDeleteColumnFromTypeScript,
	getModifyColumnOfTypeScript,
};
