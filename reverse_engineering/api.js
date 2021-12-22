'use strict';

const oracleHelper = require('./helpers/oracleHelper');
const logInfo = require('./helpers/logInfo');
const { setDependencies, dependencies } = require('./helpers/appDependencies');
let _;

module.exports = {
	async connect(connectionInfo, logger, callback, app) {
		initDependencies(app);
		logger.clear();
		logger.log('info', connectionInfo, 'connectionInfo', connectionInfo.hiddenKeys);
		try {
			await oracleHelper.connect(logger, connectionInfo);
			callback();
		} catch (err) {
			handleError(logger, err, callback);
			throw err;
		}
	},

	async disconnect(connectionInfo, logger, callback) {
		try {
			await oracleHelper.disconnect();
			callback(null);
		} catch (err) {
			handleError(logger, err, callback);
		}
	},

	async testConnection(connectionInfo, logger, callback, app) {
		try {
			logInfo('Test connection', connectionInfo, logger);
			await this.connect(connectionInfo, logger, () => { }, app);
			callback(null);
		} catch (error) {
			logger.log('error', { message: error.message, stack: error.stack, error }, 'Test connection');
			callback({ message: error.message, stack: error.stack });
		}
	},

	async getDbCollectionsNames(connectionInfo, logger, callback, app) {
		try {
			
			logInfo('Retrieving databases and tables information', connectionInfo, logger);
			await this.connect(connectionInfo, logger, () => { }, app);
			const objects = await oracleHelper.getEntitiesNames({
				info: (data) => {
					logger.log('info', data, 'Retrieving table and view names');
				},
				error: (e) => {
					logger.log('error', { message: e.message, stack: e.stack, error: e }, 'Retrieving databases and tables information');
				},
			});

			callback(null, objects);
		} catch (error) {
			logger.log('error', { message: error.message, stack: error.stack, error }, 'Retrieving databases and tables information');
			callback({ message: error.message, stack: error.stack });
		}
	},

	async getDbCollectionsData(collectionsInfo, logger, callback, app) {
		try {
			logger.log('info', collectionsInfo, 'Retrieving schema', collectionsInfo.hiddenKeys);
			initDependencies(app);
			logger.progress({ message: 'Start reverse-engineering process', containerName: '', entityName: '' });
			const data = collectionsInfo.collectionData;
			const collections = data.collections;
			const dataBaseNames = data.dataBaseNames;
			const dbVersion = await oracleHelper.getDbVersion(logger);
			const entitiesPromises = await dataBaseNames.reduce(async (packagesPromise, schema) => {
				const packages = await packagesPromise;
				const entities = oracleHelper.splitEntityNames(collections[schema]);

				const tablesPackages = await entities.tables.reduce(async (next, table) => {
					const result = await next;

					const fullTableName = oracleHelper.getFullEntityName(schema, table);
					logger.progress({ message: `Start getting data from table`, containerName: schema, entityName: table });
					const ddl = await oracleHelper.getDDL(table);
					const quantity = await oracleHelper.getRowsCount(fullTableName);

					logger.progress({ message: `Fetching record for JSON schema inference`, containerName: schema, entityName: table });

					const { documents, jsonSchema } = await oracleHelper.getJsonSchema(logger, getCount(quantity, collectionsInfo.recordSamplingSettings), fullTableName);
					const entityData = await oracleHelper.getEntityData(fullTableName);

					logger.progress({ message: `Schema inference`, containerName: schema, entityName: table });

					const handledDocuments = oracleHelper.handleComplexTypesDocuments(jsonSchema, documents);

					logger.progress({ message: `Data retrieved successfully`, containerName: schema, entityName: table });

					return result.concat({
						dbName: schema,
						collectionName: table,
						entityLevel: entityData,
						documents: handledDocuments,
						views: [],
						ddl: {
							script: ddl,
							type: 'oracle'
						},
						emptyBucket: false,
						validation: {
							jsonSchema
						},
						bucketInfo: {
							indexes: [],
							database: schema,
						}
					});
				}, Promise.resolve([]));

				const views = await entities.views.reduce(async (next, view) => {
					const result = await next;

					const fullViewName = oracleHelper.getFullEntityName(schema, view);
					logger.progress({ message: `Start getting data from view`, containerName: schema, entityName: view });
					const ddl = await oracleHelper.getViewDDL(view);
					const viewData = await oracleHelper.getViewData(fullViewName);

					logger.progress({ message: `Data retrieved successfully`, containerName: schema, entityName: view });

					return result.concat({
						name: view,
						data: viewData,
						ddl: {
							script: ddl,
							type: 'oracle'
						}
					});
				}, Promise.resolve([]));

				if (_.isEmpty(views)) {
					return [...packages, ...tablesPackages];
				}

				const viewPackage = Promise.resolve({
					dbName: schema,
					entityLevel: {},
					views,
					emptyBucket: false,
					bucketInfo: {
						indexes: [],
						database: schema,
					}
				});
				return [ ...packages, ...tablesPackages, viewPackage ];
			}, Promise.resolve([]));
			const packages = await Promise.all(entitiesPromises).catch(err => callback(err));
			callback(null, packages.filter(Boolean), { version: dbVersion });
		} catch (error) {
			logger.log('error', { message: error.message, stack: error.stack, error }, 'Reverse-engineering process failed');
			callback({ message: error.message, stack: error.stack });
		}
	}
};

const handleError = (logger, error, cb) => {
	const message = _.isString(error) ? error : _.get(error, 'message', 'Reverse Engineering error')
	logger.log('error', { error }, 'Reverse Engineering error');

	cb(message);
};

const getCount = (count, recordSamplingSettings) => {
	const per = recordSamplingSettings.relative.value;
	const size = (recordSamplingSettings.active === 'absolute')
		? recordSamplingSettings.absolute.value
		: Math.round(count / 100 * per);
	return size;
};

const initDependencies = app => {
	setDependencies(app);
	_ = dependencies.lodash;
	oracleHelper.setDependencies(dependencies);
};
