const reApi = require('../reverse_engineering/api');
const applyToInstanceHelper = require('./applyToInstanceHelper');

module.exports = {
	generateScript(data, logger, callback, app) {
		const {
			getAlterContainersScripts,
			getAlterCollectionsScripts,
			getAlterViewScripts,
			getAlterModelDefinitionsScripts,
		} = require('./helpers/alterScriptFromDeltaHelper');

		const collection = JSON.parse(data.jsonSchema);
		if (!collection) {
			throw new Error(
				'"comparisonModelCollection" is not found. Alter script can be generated only from Delta model',
			);
		}

		const dbVersion = data.modelData[0]?.dbVersion;
		const containersScripts = getAlterContainersScripts(collection);
		const collectionsScripts = getAlterCollectionsScripts(collection, app, dbVersion);
		const viewScripts = getAlterViewScripts(collection, app);
		const modelDefinitionsScripts = getAlterModelDefinitionsScripts(collection, app);

		callback(
			null,
			[...containersScripts, ...collectionsScripts, ...viewScripts, ...modelDefinitionsScripts].join('\n\n'),
		);
	},
	generateViewScript(data, logger, callback, app) {
		callback(new Error('Forward-Engineering of delta model on view level is not supported'));
	},
	generateContainerScript(data, logger, callback, app) {
		callback(new Error('Forward-Engineering of delta model on container level is not supported'));
	},
	getDatabases(connectionInfo, logger, callback, app) {
		logger.progress({ message: 'Find all schemas' });
		reApi.getSchemaNames(connectionInfo, logger, callback, app);
	},
	applyToInstance(connectionInfo, logger, callback, app) {
		logger.clear();
		logger.log('info', connectionInfo, 'connectionInfo', connectionInfo.hiddenKeys);

		applyToInstanceHelper.applyToInstance(connectionInfo, logger, app)
			.then(result => {
				callback(null, result);
			})
			.catch(error => {
				const err = {
					message: error.message,
					stack: error.stack,
				};
				logger.log('error', err, 'Error when applying to instance');

				callback(err);
			});
	},
	testConnection(connectionInfo, logger, callback, app) {
		reApi.testConnection(connectionInfo, logger, callback, app);
	}
};