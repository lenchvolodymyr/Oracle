module.exports = {
    createSchema: 'CREATE USER ${schemaName} NO AUTHENTICATION;\n',

    comment: '\nCOMMENT ON ${object} ${objectName} IS ${comment};\n',

    createTable:
        'CREATE ${tableType} TABLE ${name}\
        ${tableProps}\
        \n${options};\n${comment}${columnDescriptions}\n',

    createTableProps: '${columnDefinitions}${keyConstraints}${checkConstraints}${foreignKeyConstraints}',

    columnDefinition: '${name} ${type} ${default} ${encrypt} ${constraints}',

    createKeyConstraint: '${constraintName}${keyType}${columns}',

    createForeignKeyConstraint: '${name} FOREIGN KEY (${foreignKey}) REFERENCES ${primaryTable} (${primaryKey})',

    checkConstraint: '${name}CHECK (${expression})',

    createForeignKey:
        'ALTER TABLE ${foreignTable} ADD CONSTRAINT ${name} FOREIGN KEY (${foreignKey}) REFERENCES ${primaryTable}(${primaryKey});\n',

    createIndex: 'CREATE${indexType} INDEX ${name} ON ${tableName}${keys}${options};\n',

    createView: 'CREATE${orReplace} VIEW${force}${viewType} ${name} \n\tAS ${selectStatement};\n${comment}\n',

    viewSelectStatement: 'SELECT ${keys}\n\tFROM ${tableName}',

    createObjectType: 'CREATE OR REPLACE TYPE ${name} AS OBJECT \n(\n${properties}\n);\n',

    objectTypeColumnDefinition: '${name} ${type}',

    createCollectionType: 'CREATE OR REPLACE TYPE ${name} IS ${collectionType}${size} OF (${datatype})${notPersistable};\n',
};
