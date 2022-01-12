module.exports = ({ _, getColumnsList, checkAllKeysDeactivated, commentIfDeactivated, wrapInQuotes, assignTemplates }) => {
    const getTableType = (
        {
            duplicated,
            external,
            immutable,
            sharded,
            temporary,
            temporaryType,
            blockchain_table_clauses,
        }
    ) => {
        const blockchain = !_.isEmpty(blockchain_table_clauses);
        switch(true) {
            case temporary:
                return `${_.toUpper(temporaryType)} TEMPORARY`;
            case sharded:
                return 'SHARDED';
            case duplicated:
                return ' DUPLICATED';
            case immutable:
                return ' IMMUTABLE ';
            case blockchain:
                return `${blockchain_table_clauses.immutable ? 'IMMUTABLE' : ''} BLOCKCHAIN`;
            default:
                return '';
        }
    };
    const getTableOptions = tableData => {
        const wrap = value => (value ? `${value}\n` : '');

        const statements = [
            { key: 'blockchain_table_clauses', getValue: getBlockChainClause },
            { key: 'storage', getValue: getStorage },
            { key: 'external_table_clause', getValue: getExternalTableClause },
            { key: 'partitioning', getValue: getPartitioning },
            { key: 'selectStatement', getValue: getBasicValue('AS') },
        ]
            .map(config => wrap(config.getValue(tableData[config.key], tableData)))
            .filter(Boolean)
            .join('');

        return _.trim(statements) ? ` ${_.trim(statements)}` : '';
    };

    const getBlockChainClause = ({
        blockchain_table_retention_clause,
        blockchain_row_retention_clause,
        blockchain_hash_and_data_format_clause,
    }) => {
        return _.trim(`${blockchain_table_retention_clause || ''}` + 
            `${blockchain_row_retention_clause || ''}` + 
            `${blockchain_hash_and_data_format_clause || ''}`);
    };

    const getStorage = ({
        organization,
        tablespace,
        logging,
    }) => {
        if (organization === 'external') {
            return 'ORGANIZATION EXTERNAL';
        }
        return `ORGANIZATION ${_.toUpper(organization) || 'HEAP'}` + 
            ` ${tablespace ? `TABLESPACE ${wrapInQuotes(tablespace)}` : ''}` + 
            ` ${logging ? 'LOGGING' : 'NOLOGGING'}`;
    }

    const getExternalTableClause = (value) => {
        if (!value?.access_driver_type) {
            return '';
        }
        const { 
            access_driver_type, 
            default_directory, 
            access_parameter_type, 
            opaque_format_spec,
            clob_subquery,
            reject_limit,
            project_column,
            location,
        } = value;
        const locationList = `(${_.map(location, ({location_directory, location_specifier}) => 
            (`${location_directory ? `${location_directory}:` : ''}` + 
                `'${location_specifier}'`)).join(', ')})`;
        const accessParamType = access_parameter_type === 'NONE' ? '' : 
            `ACCESS PARAMETERS ${clob_subquery ? `USING CLOB ${clob_subquery}` : opaque_format_spec}`;
        const externalData = `${default_directory && !_.isEmpty(default_directory) ? `DEFAULT DIRECTORY ${default_directory}` : ''}` + 
            accessParamType + `${location && !_.isEmpty(location) ? `LOCATION ${locationList}` : ''}`;
        return ` (TYPE ${_.toUpper(access_driver_type)} ${externalData})` +
            ` REJECT LIMIT ${reject_limit || 'UNLIMITED'} `;
    };

    const getPartitioning = (value, { isActivated }) => {
        // if (value && value.partitionBy) {
        //     const expression = getPartitionKeys(value, isActivated);
        //     return `PARTITION BY ${value.partitionBy}${expression}`;
        // }
        return '';
    };

    const getBasicValue = prefix => value => {
        if (value) {
            return `${prefix} ${value}`;
        }
    };

    const generateConstraintsString = (dividedConstraints, isParentActivated) => {
        const deactivatedItemsAsString = commentIfDeactivated(
            (dividedConstraints?.deactivatedItems || []).join(',\n\t'),
            {
                isActivated: !isParentActivated,
                isPartOfLine: true,
            }
        );
        const activatedConstraints = dividedConstraints?.activatedItems?.length
            ? ',\n\t' + dividedConstraints.activatedItems.join(',\n\t')
            : '';

        const deactivatedConstraints = dividedConstraints?.deactivatedItems?.length
            ? '\n\t' + deactivatedItemsAsString
            : '';

        return activatedConstraints + deactivatedConstraints;
    };

    const foreignKeysToString = keys => {
        if (Array.isArray(keys)) {
            const activatedKeys = keys
                .filter(key => _.get(key, 'isActivated', true))
                .map(key => wrapInQuotes(_.trim(key.name)));
            const deactivatedKeys = keys
                .filter(key => !_.get(key, 'isActivated', true))
                .map(key => wrapInQuotes(_.trim(key.name)));
            const deactivatedKeysAsString = deactivatedKeys.length
                ? commentIfDeactivated(deactivatedKeys, { isActivated: false, isPartOfLine: true })
                : '';

            return activatedKeys.join(', ') + deactivatedKeysAsString;
        }
        return keys;
    };

    const foreignActiveKeysToString = keys => {
        return keys.map(key => _.trim(key.name)).join(', ');
    };

    const createKeyConstraint = (templates, isParentActivated) => keyData => {
        const constraintName = wrapInQuotes(_.trim(keyData.name));
        const isAllColumnsDeactivated = checkAllKeysDeactivated(keyData.columns);
        const columns = getColumnsList(keyData.columns, isAllColumnsDeactivated, isParentActivated);
        
        return {
            statement: assignTemplates(templates.createKeyConstraint, {
                constraintName: keyData.name ? `CONSTRAINT ${wrapInQuotes(constraintName)} ` : '',
                keyType: keyData.keyType,
                columns,
            }),
            isActivated: !isAllColumnsDeactivated,
        };
    };

    return {
        getTableOptions,
        getTableType,
        generateConstraintsString,
        foreignKeysToString,
        foreignActiveKeysToString,
        createKeyConstraint,
    };
};
