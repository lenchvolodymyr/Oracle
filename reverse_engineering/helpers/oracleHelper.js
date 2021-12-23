const oracleDB = require('oracledb');

const noConnectionError = { message: 'Connection error' };

const setDependencies = ({ lodash }) => _ = lodash;

let connection;

const connect = async (logger, { host, port, userName, userPassword, databaseName, clientPath }) => {
	if (!connection) {
		oracleDB.initOracleClient({ libDir: clientPath });

		return authByCredentials({ host: `${host}:${port}`, username: userName, password: userPassword, database: databaseName });
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

const authByCredentials = ({ host, username, password, database }) => {
	return new Promise((resolve, reject) => {
		oracleDB.getConnection({
			username,
			password,
			connectString: `${host}/${database}`,
		}, (err, conn) => {
			if (err) {
				connection = null;
				return reject(err);
			}
			connection = conn;
			resolve();
		});
	});
};

const pairToObj = (pairs) => _.reduce(pairs, (obj, pair) => ({ ...obj, [pair[0]]: [...(obj[pair[0]] || []), pair[1]] }), {});

const tableNamesByUser = () => execute('SELECT OWNER, TABLE_NAME FROM ALL_TABLES WHERE OWNER NOT LIKE \'%SYS%\' AND OWNER NOT LIKE \'%XDB%\'');
const externalTableNamesByUser = () => execute('SELECT OWNER, TABLE_NAME FROM ALL_EXTERNAL_TABLES WHERE OWNER NOT LIKE \'%SYS%\' AND OWNER NOT LIKE \'%XDB%\'');
const viewNamesByUser = () => execute('SELECT OWNER, VIEW_NAME || \' (v)\' FROM ALL_VIEWS WHERE OWNER NOT LIKE \'%SYS%\' AND OWNER NOT LIKE \'%XDB%\'');
const materializedViewNamesByUser = () => execute('SELECT OWNER, VIEW_NAME || \' (v)\' FROM ALL_MVIEWS WHERE OWNER NOT LIKE \'%SYS%\' AND OWNER NOT LIKE \'%XDB%\'');

const getEntitiesNames = async (logger) => {
	const tables = await tableNamesByUser().catch(e => {
		logger.info({ message: 'Cannot retrieve tables' });
		logger.error(e);
		return [];
	});
	const externalTables = await externalTableNamesByUser().catch(e => {
		logger.info({ message: 'Cannot retrieve external tables' });
		logger.error(e);

		return [];
	});
	const views = await viewNamesByUser().catch(e => {
		logger.info({ message: 'Cannot retrieve views' });
		logger.error(e);

		return [];
	});
	const materializedViews = await materializedViewNamesByUser().catch(e => {
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

const execute = (command, options = {}) => {
	if (!connection) {
		return Promise.reject(noConnectionError)
	}
	return new Promise((resolve, reject) => {
		connection.execute(
			command,
			{},
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

const addQuotes = string => {
	if (/^\".*\"$/.test(string)) {
		return string;
	}
	return `"${string}""`;
};

const getDDL = async tableName => {
	try {
		//TODO what if external table?
		const queryResult = await execute(`SELECT DBMS_METADATA.GET_DDL('TABLE', TABLE_NAME) FROM ALL_TABLES WHERE TABLE_NAME='${tableName}'`);
		const ddl = await _.first(_.first(queryResult)).getData();

		if (!/;\s*$/.test(ddl)) {
			return `${ddl};`;
		}

		return ddl;
	} catch (err) {
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

const selectRecords = async ({ tableName, limit, jsonColumns }) => {
	const records = await execute(`SELECT ${jsonColumns.map((c) => c['COLUMN_NAME']).join(', ')} FROM ${tableName} FETCH NEXT ${limit} ROWS ONLY`, {
		outFormat: oracleDB.OBJECT,
	});

	const result = await readRecordsValues(records);

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

	let indexQuery = `SELECT DBMS_METADATA.GET_DDL('INDEX',u.index_name) AS STATEMENT FROM ALL_INDEXES u WHERE u.TABLE_OWNER='${schema}' AND u.TABLE_NAME='${table}'`;

	if (primaryKeyConstraints.length) {
		indexQuery += ` AND u.INDEX_NAME NOT IN ('${primaryKeyConstraints.join('\', \'')}')`;
	}
	const indexRecords = await execute(indexQuery, { outFormat: oracleDB.OBJECT });
	const indexStatements = await readRecordsValues(indexRecords)

	return indexStatements.map(s => `${s['STATEMENT']};`);
};

const handleComplexTypesDocuments = (jsonSchema, documents) => {
	try {
		//TODO implement handling
		return [];
	} catch (err) {
		return documents;
	}
};

const getViewDDL = async viewName => {
	try {
		//TODO what if mat. view?
		const queryResult = await execute(`SELECT DBMS_METADATA.GET_DDL('VIEW', VIEW_NAME) FROM ALL_VIEWS WHERE VIEW_NAME='${viewName}'`);
		return await _.first(_.first(queryResult)).getData();
	} catch (err) {
		return '';
	}
};

const getViewData = async fullName => {
	const [schemaName, viewName] = fullName.split('.');

	try {
		return {};
	} catch (err) {
		return {};
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
	handleComplexTypesDocuments,
	getViewDDL,
	getViewData,
	getDbVersion,
	getJsonColumns,
	selectRecords,
};