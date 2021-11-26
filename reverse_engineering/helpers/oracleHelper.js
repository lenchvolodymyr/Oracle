const oracleDB = require('oracledb');

const noConnectionError = { message: 'Connection error' };

const setDependencies = ({ lodash }) => _ = lodash;

let connection;
let containers = {};

const connect = async (logger, { host, port, userName, userPassword, databaseName, clientPath }) => {
    if (!connection) {
        oracleDB.initOracleClient({libDir: clientPath});
        return authByCredentials({ host: `${host}:${port}`, username: userName, password: userPassword, database: databaseName});
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

const pairToObj = (pairs) => _.reduce(pairs, (obj, pair) => ({...obj, [pair[0]]: [...(obj[pair[0]] || []), pair[1]]}), {});

const tableNamesByUser = () => execute('SELECT OWNER, TABLE_NAME FROM ALL_TABLES WHERE OWNER NOT LIKE \'%SYS%\' AND OWNER NOT LIKE \'%XDB%\'');
const externalTableNamesByUser = () => execute('SELECT OWNER, TABLE_NAME FROM ALL_EXTERNAL_TABLES WHERE OWNER NOT LIKE \'%SYS%\' AND OWNER NOT LIKE \'%XDB%\'');
const viewNamesByUser = () => execute('SELECT OWNER, VIEW_NAME || \' (v)\' FROM ALL_VIEWS WHERE OWNER NOT LIKE \'%SYS%\' AND OWNER NOT LIKE \'%XDB%\'');
const materializedViewNamesByUser = () => execute('SELECT OWNER, VIEW_NAME || \' (v)\' FROM ALL_MVIEWS WHERE OWNER NOT LIKE \'%SYS%\' AND OWNER NOT LIKE \'%XDB%\'');

const getEntitiesNames = async () => {
    const tables = await tableNamesByUser().catch(e => []);
    const externalTables = await externalTableNamesByUser().catch(e => []);
	const views = await viewNamesByUser().catch(e => []);
	const materializedViews = await materializedViewNamesByUser().catch(e => []);

	const entities = pairToObj([...tables, ...externalTables, ...views, ...materializedViews]);

    return Object.keys(entities).reduce((arr, user) => [...arr, {
            dbName: user,
			dbCollections: entities[user],
			isEmpty: !entities[user].length
        }], []);;
};

const execute = command => {
	if (!connection) {
		return Promise.reject(noConnectionError)
	}
	return new Promise((resolve, reject) => {
		connection.execute(
            command,
			(err, result) => {
				if (err) {
					return reject(err);
				}
				resolve(result.rows)
			}
		);
	});
};
const isView = name => name.slice(-4) === ' (v)';
const splitEntityNames = names => {
	const namesByCategory = _.partition(names, isView);

	return { views: namesByCategory[0].map(name => name.slice(0, -4)), tables: namesByCategory[1] };
};

const getContainerData = async schema => {
	if (containers[schema]) {
		return containers[schema];
	}

	try {
        //TODO what data need to be returned?
		const data = {};
		containers[schema] = data;
		return data;
	} catch (err) {
		return {};
	}
}

const getFullEntityName = (schemaName, tableName) => {
	return  [schemaName, tableName].map(addQuotes).join('.');
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
		debugger;
		return await _.first(_.first(queryResult)).getData();
	} catch (err) {
		return '';
	}
};

const getRowsCount = async tableName => {
	try {
		const queryResult = await execute(`SELECT count(*) AS COUNT FROM ${tableName};`);

		return _.first(queryResult);
	} catch {
		return '';
	}
};

const getJsonSchema = async (logger, limit, tableName) => {
	//TODO implement json schema retrieval
	return {
		jsonSchema: { properties: {} },
		documents: [],
	};
};

const getEntityData = async fullName => {
	const [schemaName, tableName] = fullName.split('.');

	try {
        //TODO implement data retrieval
		return {};
	} catch (err) {
		return {};
	}
};

const handleComplexTypesDocuments = (jsonSchema, documents) => {
	try {
        //TODO implement handling
		return {};
	} catch (err) {
		return documents;
	}
};

const getViewDDL = async viewName => {
	try {
        //TODO what if mat. view?
		const queryResult = await execute(`SELECT DBMS_METADATA.GET_DDL('VIEW', VIEW_NAME) FROM ALL_VIEWS WHERE VIEW_NAME='${viewName}'`);
		return await _.first(queryResult).getData();
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
    getContainerData,
    getFullEntityName,
    getDDL,
    getRowsCount,
    getJsonSchema,
    getEntityData,
    handleComplexTypesDocuments,
    getViewDDL,
    getViewData
};