const oracleDB = require('oracledb');

const noConnectionError = { message: 'Connection error' };

const setDependencies = ({ lodash }) => _ = lodash;

let connection;

const connect = async ({ connectionMethod, authMethod, host, port, userName, userPassword, databaseName, serviceName, clientPath, queryRequestTimeout }) => {
	if (!connection) {
		oracleDB.initOracleClient({ libDir: clientPath });

		const connectString = connectionMethod === 'Wallet' ? serviceName : `${host}:${port}/${databaseName}`;

		return authByCredentials({ connectString, username: userName, password: userPassword, queryRequestTimeout });
	}
};

const disconnect = async () => {
	if (!connection) {
		return Promise.reject(noConnectionError);
	}
	return new Promise((resolve, reject) => {
		connection.close(err => {
			connection = null;
			if (err) {
				return reject(err);
			}
			resolve();
		});
	});
};

const authByCredentials = ({ connectString, username, password, queryRequestTimeout }) => {
	return new Promise((resolve, reject) => {
		oracleDB.getConnection({
			username,
			password,
			connectString,
		}, (err, conn) => {
			if (err) {
				connection = null;
				return reject(err);
			}
			try {
				conn.callTimeout = Number(queryRequestTimeout || 0);
				connection = conn;
				resolve();
			} catch (err) {
				reject(err);
			}
		});
	});
};

const pairToObj = (pairs) => _.reduce(pairs, (obj, pair) => ({ ...obj, [pair[0]]: [...(obj[pair[0]] || []), pair[1]] }), {});

const selectEntities = (selectStatement, includeSystemCollection, userName) => {
	if (includeSystemCollection) {
		return execute(selectStatement);
	} else {
		return execute(`${selectStatement} WHERE T.OWNER = :userName`, {}, [userName]);
	}
};

const tableNamesByUser = ({includeSystemCollection }, userName) => selectEntities(`SELECT T.OWNER, T.TABLE_NAME FROM ALL_TABLES T`, includeSystemCollection, userName);
const externalTableNamesByUser = ({includeSystemCollection }, userName) => selectEntities(`SELECT T.OWNER, T.TABLE_NAME FROM ALL_EXTERNAL_TABLES T`, includeSystemCollection, userName);
const viewNamesByUser = ({includeSystemCollection }, userName) => selectEntities(`SELECT T.OWNER, T.VIEW_NAME || \' (v)\' FROM ALL_VIEWS T`, includeSystemCollection, userName);
const materializedViewNamesByUser = ({includeSystemCollection }, userName) => selectEntities(`SELECT T.OWNER, T.VIEW_NAME || \' (v)\' FROM ALL_MVIEWS T`, includeSystemCollection, userName);

const getCurrentUserName = async () => {
	const currentUser = await execute(`SELECT USER FROM DUAL`, { outFormat: oracleDB.OBJECT });

	return currentUser?.[0]?.USER;
};

const getEntitiesNames = async (connectionInfo,logger) => {
	const currentUser = await getCurrentUserName();
	const tables = await tableNamesByUser(connectionInfo, currentUser).catch(e => {
		logger.info({ message: 'Cannot retrieve tables' });
		logger.error(e);
		return [];
	});
	const externalTables = await externalTableNamesByUser(connectionInfo, currentUser).catch(e => {
		logger.info({ message: 'Cannot retrieve external tables' });
		logger.error(e);

		return [];
	});
	const views = await viewNamesByUser(connectionInfo, currentUser).catch(e => {
		logger.info({ message: 'Cannot retrieve views' });
		logger.error(e);

		return [];
	});
	const materializedViews = await materializedViewNamesByUser(connectionInfo, currentUser).catch(e => {
		logger.info({ message: 'Cannot retrieve materialized views' });
		logger.error(e);

		return [];
	});
	const entities = pairToObj([...tables, ...externalTables, ...views, ...materializedViews]);

	return Object.keys(entities).reduce((arr, user) => [...arr, {
		dbName: user,
		dbCollections: entities[user],
		isEmpty: !entities[user].length
	}], []);
};

const execute = (command, options = {}, binds = []) => {
	if (!connection) {
		return Promise.reject(noConnectionError)
	}
	return new Promise((resolve, reject) => {
		connection.execute(
			command,
			binds,
			options,
			(err, result) => {
				if (err) {
					return reject(err);
				}
				resolve(result.rows)
			}
		);
	});
};

const getDbVersion = async (logger) => {
	try {
		const version = await execute('SELECT VERSION FROM PRODUCT_COMPONENT_VERSION WHERE product LIKE \'Oracle Database%\'');

		logger.log('info', version, 'DB Version');

		if (!version?.[0]?.[0]) {
			return '21c';
		}

		const v = version[0][0].split('.').shift() + 'c';
		const versions = [
			"12c",
			"18c",
			"19c",
			"21c"
		];

		if (!versions.includes(v)) {
			return '21c';
		}
		
		return v;
	} catch (e) {
		logger.log('error', { message: e.message, stack: e.stack }, 'Error of getting DB Version');
		return '21c';
	}
};

const isView = name => name.slice(-4) === ' (v)';
const splitEntityNames = names => {
	const namesByCategory = _.partition(names, isView);

	return { views: namesByCategory[0].map(name => name.slice(0, -4)), tables: namesByCategory[1] };
};

const getDDL = async (tableName, logger) => {
	try {
		const queryResult = await execute(`SELECT DBMS_METADATA.GET_DDL('TABLE', TABLE_NAME, OWNER) FROM ALL_TABLES WHERE TABLE_NAME='${tableName}'`);
		const ddl = await _.first(_.first(queryResult)).getData();

		if (!/;\s*$/.test(ddl)) {
			return `${ddl};`;
		}

		return ddl;
	} catch (err) {
		logger.log('error', {
			message: 'Cannot get DDL for table: ' + tableName,
			error: { message: err.message, stack: err.stack, err: _.omit(err, ['message', 'stack']) }
		}, 'Getting DDL');
		return '';
	}
};

const getJsonColumns = async tableName => {
	const result = await execute(`SELECT * FROM all_tab_columns WHERE TABLE_NAME='${tableName}' AND DATA_TYPE IN ('CLOB', 'BLOB', 'NVARCHAR2', 'JSON')`, {
		outFormat: oracleDB.OBJECT,
	});

	return result;
};

const getRowsCount = async tableName => {
	try {
		const queryResult = await execute(`SELECT count(*) AS COUNT FROM ${tableName}`);

		return Number(_.first(queryResult.flat()) || 0);
	} catch (error) {
		return 0;
	}
};

const readLobs = (record) => {
	return Object.keys(record).reduce(async (prev, key) => {
		const result = await prev;
		let value = record[key]; 
		
		if (value instanceof oracleDB.Lob) {
			value = await value.getData();
		}

		if (value instanceof Buffer) {
			value = value.toString();
		}

		result[key] = value;

		return result;
	}, Promise.resolve({}));
};

const readRecordsValues = async (records) => {
	return await records.reduce(async (prev, record) => {
		const result = await prev;
		
		const updatedRecord = await readLobs(record);

		return result.concat(updatedRecord);
	}, Promise.resolve([]));
};

const escapeName = (name) => {
	if (name.includes(' ')) {
		return `'${name}'`;
	}

	return name;
};

const replaceNames = (columns, records) => {
	return records.map((record) => {
		return columns.reduce((result, column) => {
			const name = column['COLUMN_NAME'];
			result[name] = record[name];

			return result;
		}, {});
	});
};

const selectRecords = async ({ tableName, limit, jsonColumns }) => {
	const records = await execute(`SELECT ${jsonColumns.map((c) => escapeName(c['COLUMN_NAME'])).join(', ')} FROM ${tableName} FETCH NEXT ${limit} ROWS ONLY`, {
		outFormat: oracleDB.OBJECT,
	});

	const result = await readRecordsValues(replaceNames(jsonColumns, records));

	return result;
};

const getJsonType = (records, columnName) => {
	return records.reduce((type, record) => {
		if (type) {
			return type;
		}
		
		try {
			const result = JSON.parse(record[columnName]);

			if (Array.isArray(result)) {
				return 'array';
			}

			if (result && typeof result === 'object') {
				return 'object';
			}

			return type;
		} catch {
			return type;
		}
	}, '');
};

const getJsonSchema = async (jsonColumns, records) => {
	const types = {
		CLOB: { type: 'lobs', mode: 'clob' },
		BLOB: { type: 'lobs', mode: 'blob' },
		NVARCHAR2: { type: 'char', mode: 'nvarchar2' },
		JSON: { type: 'JSON' }
	};
	const properties = jsonColumns.reduce((properties, column) => {
		const columnName = column['COLUMN_NAME'];
		const columnType = column['DATA_TYPE'];
		const schema = types[columnType];

		if (!schema) {
			return properties;
		}

		const subtype = getJsonType(records, columnName);

		if (!subtype) {
			return properties;
		}

		return {
			...properties,
			[columnName]: {
				...schema,
				subtype,
			}
		};
	}, {});

	return { properties };
};

const getIndexStatements = async ({ table, schema }) => {
	let primaryKeyConstraints = await execute(`SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE CONSTRAINT_TYPE='P' AND OWNER='${schema}' AND TABLE_NAME='${table}'`);

	primaryKeyConstraints = primaryKeyConstraints.flat();

	let indexQuery = `SELECT DBMS_METADATA.GET_DDL('INDEX',u.index_name, OWNER) AS STATEMENT FROM ALL_INDEXES u WHERE u.TABLE_OWNER='${schema}' AND u.TABLE_NAME='${table}'`;

	if (primaryKeyConstraints.length) {
		indexQuery += ` AND u.INDEX_NAME NOT IN ('${primaryKeyConstraints.join('\', \'')}')`;
	}
	const indexRecords = await execute(indexQuery, { outFormat: oracleDB.OBJECT });
	const indexStatements = await readRecordsValues(indexRecords)

	return indexStatements.map(s => `${s['STATEMENT']};`);
};

const getViewDDL = async (viewName, logger) => {
	try {
		const queryResult = await execute(`SELECT DBMS_METADATA.GET_DDL('VIEW', VIEW_NAME, OWNER) FROM ALL_VIEWS WHERE VIEW_NAME='${viewName}'`);

		return `${(await _.first(_.first(queryResult)).getData())};`;
	} catch (err) {
		logger.log('error', {
			message: 'Cannot get DDL for view: ' + viewName,
			error: { message: err.message, stack: err.stack, err: _.omit(err, ['message', 'stack']) }
		}, 'Getting DDL');
		return '';
	}
};

module.exports = {
	connect,
	disconnect,
	setDependencies,
	getEntitiesNames,
	splitEntityNames,
	getDDL,
	getRowsCount,
	getJsonSchema,
	getIndexStatements,
	getViewDDL,
	getDbVersion,
	getJsonColumns,
	selectRecords,
};