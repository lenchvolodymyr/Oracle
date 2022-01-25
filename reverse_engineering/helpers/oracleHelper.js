const oracleDB = require('oracledb');
const extractWallet = require('./extractWallet');
const path = require('path');
const fs = require('fs');
const parseTns = require('./parseTns');
const ssh = require('tunnel-ssh');

const noConnectionError = { message: 'Connection error' };

const setDependencies = ({ lodash }) => _ = lodash;

let connection;
let sshTunnel;

const getSshConfig = (info) => {
	const config = {
		username: info.ssh_user,
		host: info.ssh_host,
		port: info.ssh_port,
		dstHost: info.host,
		dstPort: info.port,
		localHost: '127.0.0.1',
		localPort: info.port,
		keepAlive: true
	};

	if (info.ssh_method === 'privateKey') {
		return Object.assign({}, config, {
			privateKey: fs.readFileSync(info.ssh_key_file),
			passphrase: info.ssh_key_passphrase
		});
	} else {
		return Object.assign({}, config, {
			password: info.ssh_password
		});
	}
};

const connectViaSsh = (info) => new Promise((resolve, reject) => {
	ssh(getSshConfig(info), (err, tunnel) => {
		if (err) {
			reject(err);
		} else {
			resolve({
				tunnel,
				info: Object.assign({}, info, {
					host: 'localhost',
				})
			});
		}
	});
});

const parseProxyOptions = (proxyString = '') => {
	const result = proxyString.match(/http:\/\/(?:.*?:.*?@)?(.*?):(\d+)/i);

	if (!result) {
		return {};
	}

	return {
		httpsProxy: result[1],
		httpsProxyPort: result[2],
	};
};

const getTnsNamesOraFile = (configDir) => {
	return [
		configDir,
		process.env.TNS_ADMIN,
		path.join(process.env.ORACLE_HOME || '', 'network', 'admin'),
		path.join(process.env.LD_LIBRARY_PATH || '', 'network', 'admin'),
	].reduce((filePath, configFolder) => {
		if (filePath) {
			return filePath;
		}

		let file = path.join(configFolder, 'tnsnames.ora');

		if (fs.existsSync(file)) {
			return file;
		} else {
			return filePath;
		}
	}, '');
};

const parseTnsNamesOra = (filePath) => {
	const content = fs.readFileSync(filePath).toString();
	const result = parseTns(content);
	return result;
};

const getConnectionStringByTnsNames = (configDir, serviceName, proxy, logger) => {
	const filePath = getTnsNamesOraFile(configDir);

	if (!fs.existsSync(filePath)) {
		return serviceName;
	}

	logger({ message: 'Found tnsnames.ora file: ' + filePath });

	const tnsData = parseTnsNamesOra(filePath);

	logger({ message: 'tnsnames.ora successfully parsed' });

	if (!tnsData[serviceName]) {
		logger({ message: 'Cannot find "' + serviceName + '" in tnsnames.ora' });

		return serviceName;
	}

	const address = tnsData[serviceName]?.data?.description?.address;
	const service = tnsData[serviceName]?.data?.description?.connect_data?.service_name;
	const sid = tnsData[data.serviceName]?.data?.description?.connect_data?.sid;

	logger({ message: 'tnsnames.ora', address, service });

	return getConnectionDescription(_.omitBy({
		...address,
		...proxy,
		protocol: address?.protocol || 'tcps',
		service: service || serviceName,
		sid: sid,
	}, logger, _.isUndefined));
};

const combine = (val, str) => val ? str : '';

const getConnectionDescription = ({
	protocol,
	host,
	port,
	sid,
	service,
	httpsProxy,
	httpsProxyPort,
}, logger) => {
	const connectionString = `(DESCRIPTION=
		(ADDRESS=
			(PROTOCOL=${protocol || 'tcp'})
			(HOST=${host})
			(PORT=${port}))
			${combine(httpsProxy, `(HTTPS_PROXY=${httpsProxy})`)}
			${combine(httpsProxyPort, `(HTTPS_PROXY_PORT=${httpsProxyPort})`)}
		(CONNECT_DATA=
					${combine(sid, `(SID=${sid})`)}
					${combine(service, `(SERVICE_NAME=${service})`)}
		)
	)`
	logger({ message: 'connectionString', connectionString });
	return connectionString;
};

const getSshConnectionString = async (data, logger) => {
	let connectionData = {
		protocol: '',
		host: '',
		port: '',
		service: '',
	};
	
	if (['Wallet', 'TNS'].includes(data.connectionMethod)) {
		const filePath = getTnsNamesOraFile(data.configDir);

		if (!fs.existsSync(filePath)) {
			throw new Error('Cannot find tnsnames.ora file. Please, specify tnsnames folder or use Base connection method.');
		}

		logger({ message: 'Found tnsnames.ora file: ' + filePath });

		const tnsData = parseTnsNamesOra(filePath);

		if (!tnsData[data.serviceName]) {
			throw new Error('Cannot find "' + data.serviceName + '" in tnsnames.ora');
		}

		const address = tnsData[data.serviceName]?.data?.description?.address;
		const service = tnsData[data.serviceName]?.data?.description?.connect_data?.service_name;
		const sid = tnsData[data.serviceName]?.data?.description?.connect_data?.sid;

		logger({ message: 'tnsnames.ora', address, service });


		connectionData.protocol = address?.protocol;
		connectionData.host = address?.host;
		connectionData.port = address?.port;
		connectionData.service = service || data.serviceName;
		connectionData.sid = sid;
	} else {
		connectionData.host = data.host;
		connectionData.port = data.port;
		connectionData.service = data.serviceName,
		connectionData.sid = data.sid;
		}

	const { tunnel, info } = await connectViaSsh({
		...data.sshConfig,
		host: connectionData.host,
		port: connectionData.port,
	});

	sshTunnel = tunnel;

	return getConnectionDescription({
		...connectionData,
		host: info.host,
		port: info.port,
	}, logger);
};

const connect = async ({
	walletFile,
	tempFolder,
	name,
	connectionMethod,
	TNSpath,
	host,
	port,
	userName,
	userPassword,
	serviceName,
	clientPath,
	clientType,
	queryRequestTimeout,
	authMethod,
	options,
	sid,
	ssh,
	ssh_user,
	ssh_host,
	ssh_port,
	ssh_method,
	ssh_key_file,
	ssh_key_passphrase,
	ssh_password,
	authRole,
}, logger) => {
	if (connection) {
		return connection;
	}
	let configDir;
	let libDir;
	let credentials = {};
	let proxy = '';

	if (connectionMethod === 'Wallet') {
		configDir = await extractWallet({ walletFile, tempFolder, name });
	}

	if (connectionMethod === 'TNS') {
		configDir = TNSpath;
	}

	if (clientType === 'InstantClient') {
		libDir = clientPath;
	}

	if (options?.proxy) {
		proxy = parseProxyOptions(options?.proxy);
	}

	oracleDB.initOracleClient({ libDir, configDir });

	let connectString = '';

	if (['Wallet', 'TNS'].includes(connectionMethod)) {
		if (proxy) {
			connectString = getConnectionStringByTnsNames(configDir, serviceName, proxy, logger);
		} else {
			connectString = serviceName;
		}
	} else {
		connectString = getConnectionDescription({
			host,
			port,
			sid,
			service: serviceName,
		}, logger);
	}

	if (ssh) {
		connectString = await getSshConnectionString({
			host,
			port,
			configDir,
			serviceName,
			sid,
			connectionMethod,
			sshConfig: {
				ssh_user,
				ssh_host,
				ssh_port,
				ssh_method,
				ssh_key_file,
				ssh_password,
				ssh_key_passphrase,
			},
		}, logger);
	}

	if (authMethod === 'OS') {
		credentials.externalAuth = true;		
	} else if (authMethod === 'Kerberos') {
		credentials.username = userName;
		credentials.password = userPassword;		
		credentials.externalAuth = true;		
	} else {
		credentials.username = userName;
		credentials.password = userPassword;
	}

	return authByCredentials({ connectString, username: userName, password: userPassword, queryRequestTimeout, authRole  });
};

const disconnect = async () => {
	if (!connection) {
		return Promise.reject(noConnectionError);
	}

	if (sshTunnel) {
		sshTunnel.close();
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

const authByCredentials = ({ connectString, username, password, queryRequestTimeout, authRole }) => {
	return new Promise((resolve, reject) => {
		oracleDB.getConnection({
			username,
			password,
			connectString,
			privilege: authRole === 'default' ? undefined : oracleDB[authRole],
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

const selectEntities = (selectStatement, includeSystemCollection, schemaName) => {
	let stmt = '';
	if (schemaName) {
		stmt = `T.OWNER = '${schemaName}'`;
	}
	if (includeSystemCollection) {
		return execute(`${selectStatement}${stmt ? ` WHERE ${stmt}`: ''}`);
	} else {
		return execute(`${selectStatement} INNER JOIN ALL_USERS U ON T.OWNER = U.USERNAME WHERE U.ORACLE_MAINTAINED = 'N'${stmt ? ` AND ${stmt}`: ''}`);
	}
};

const tableNamesByUser = ({includeSystemCollection, schemaName }) => selectEntities(`SELECT T.OWNER, T.TABLE_NAME FROM ALL_TABLES T`, includeSystemCollection, schemaName);
const externalTableNamesByUser = ({includeSystemCollection, schemaName }) => selectEntities(`SELECT T.OWNER, T.TABLE_NAME FROM ALL_EXTERNAL_TABLES T`, includeSystemCollection, schemaName);
const viewNamesByUser = ({includeSystemCollection, schemaName }) => selectEntities(`SELECT T.OWNER, T.VIEW_NAME || \' (v)\' FROM ALL_VIEWS T`, includeSystemCollection, schemaName);
const materializedViewNamesByUser = ({includeSystemCollection, schemaName }) => selectEntities(`SELECT T.OWNER, T.MVIEW_NAME || \' (v)\' FROM ALL_MVIEWS T`, includeSystemCollection, schemaName);

const getEntitiesNames = async (connectionInfo,logger) => {
	const tables = await tableNamesByUser(connectionInfo).catch(e => {
		logger.info({ message: 'Cannot retrieve tables' });
		logger.error(e);
		return [];
	});

	logger.info({ tables });

	const externalTables = await externalTableNamesByUser(connectionInfo).catch(e => {
		logger.info({ message: 'Cannot retrieve external tables' });
		logger.error(e);

		return [];
	});

	logger.info({ externalTables });

	const views = await viewNamesByUser(connectionInfo).catch(e => {
		logger.info({ message: 'Cannot retrieve views' });
		logger.error(e);

		return [];
	});

	logger.info({ views });

	const materializedViews = await materializedViewNamesByUser(connectionInfo).catch(e => {
		logger.info({ message: 'Cannot retrieve materialized views' });
		logger.error(e);

		return [];
	});

	logger.info({ materializedViews });

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

const getDDL = async (tableName, schema, logger) => {
	try {
		const queryResult = await execute(`
			SELECT DBMS_METADATA.GET_DDL('TABLE', TABLE_NAME, OWNER) || ';' || LISTAGG(STATEMENT, ';') WITHIN GROUP (ORDER BY STATEMENT) AS ddl,
				NVL(NUM_ROWS,0) AS n_rows,
				(SELECT LISTAGG(COLUMN_NAME, ',') || ':' || LISTAGG(DATA_TYPE, ',')
				FROM ALL_TAB_COLUMNS 
				WHERE TABLE_NAME='${tableName}' 
				AND OWNER='${schema}' 
				AND DATA_TYPE IN ('CLOB', 'BLOB', 'NVARCHAR2', 'JSON')
				GROUP BY OWNER, TABLE_NAME) AS json_columns
			FROM ALL_TABLES LEFT JOIN (
				SELECT DBMS_METADATA.GET_DDL('INDEX',u.index_name, OWNER) || ';' AS STATEMENT, TABLE_NAME AS TN
				FROM ALL_INDEXES u
				WHERE u.TABLE_OWNER='${schema}' 
				AND u.TABLE_NAME='${tableName}'
				AND u.INDEX_NAME NOT IN (
					SELECT CONSTRAINT_NAME 
					FROM ALL_CONSTRAINTS 
					WHERE CONSTRAINT_TYPE='P' 
					AND OWNER='${schema}' 
					AND TABLE_NAME='${tableName}'
				)) 
			ON TABLE_NAME=TN 
			WHERE OWNER='${schema}' AND TABLE_NAME='${tableName}'
			GROUP BY OWNER, TABLE_NAME, NUM_ROWS
		`);
		const row = _.first(queryResult);
		const ddl = await _.first(row).getData();
		const countOfRecords = row[1] || 0;
		const namesAndTypes = row[2] ? row[2].split(':') : [];
		let jsonColumns = {};
		if (!_.isEmpty(namesAndTypes)) {
			jsonColumns = _.zipObject(namesAndTypes[0].split(','), namesAndTypes[1].split(','));
		}
		const queryObj = {
			ddl,
			jsonColumns,
			countOfRecords,
			};
		logger.log('info', queryObj, `Getting DDL from "${schema}"."${tableName}"`);
		return queryObj;
	} catch (err) {
		logger.log('error', {
			message: 'Cannot get DDL for table: ' + tableName,
			error: { message: err.message, stack: err.stack, err: _.omit(err, ['message', 'stack']) }
		}, `Getting DDL from "${schema}"."${tableName}"`);
		return {
			ddl: '',
			jsonColumns: {},
			countOfRecords: 0,
		};
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
	if (/[\s\da-z]/.test(name)) {
		return `"${name}"`;
	}

	return name;
};

const replaceNames = (names, records) => {
	return records.map((record) => {
		return names.reduce((result, name) => {
			result[name] = record[name];
			return result;
		}, {});
	});
};

const selectRecords = async ({ tableName, limit, jsonColumns, schema }) => {
	const names = Object.keys(jsonColumns);
	const records = await execute(`SELECT ${names.map((c) => escapeName(c)).join(', ')} FROM ${escapeName(schema)}.${escapeName(tableName)} FETCH NEXT ${limit} ROWS ONLY`, {
		outFormat: oracleDB.OBJECT,
	});
	const result = await readRecordsValues(replaceNames(names, records));
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
		JSON: { type: 'json' }
	};
	const properties = Object.keys(jsonColumns).reduce((properties, key) => {
		const columnName = key;
		const columnType = jsonColumns?.key;
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

const logEnvironment = (logger) => {
	logger.log('info', {
		TNS_ADMIN: process.env.TNS_ADMIN ?? '',
		ORACLE_HOME: process.env.ORACLE_HOME ?? '',
		LD_LIBRARY_PATH: process.env.LD_LIBRARY_PATH ?? '',
		ORACLE_BASE: process.env.ORACLE_BASE ?? '',
	}, 'Environment variables');
};

module.exports = {
	connect,
	disconnect,
	setDependencies,
	getEntitiesNames,
	splitEntityNames,
	getDDL,
	getJsonSchema,
	getViewDDL,
	getDbVersion,
	selectRecords,
	logEnvironment,
	execute,
};