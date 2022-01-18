module.exports = ({ _, wrapInQuotes }) => {
    const getIndexType = (indexType) => {
        return indexType ? ` ${_.toUpper(indexType)}` : '';
    };

    const getIndexKeys = ({
        indxKey,
        column_expression,
    }) => {
        if (_.isArray(indxKey) && !_.isEmpty(indxKey)) {
            return `\n(\n\t${_.map(indxKey, ({name, type}) => `${wrapInQuotes(name)} ${_.toUpper(type)}`).join(',\n\t')}\n)\n\t`;
        }
        return _.isEmpty(column_expression) ? '' : `(${_.map(column_expression, expr => expr.value)})`;
    };

    const getIndexOptions = ({
        indxDescription,
        comments,
        tablespace,
        index_properties,
        index_attributes,
        index_compression,
        logging_clause,
    }) => {
        if (index_properties) {
            return ` ${index_properties}`;
        } else if (index_attributes) {
            return ` ${index_attributes}`;
        }
        return `${logging_clause ? ` ${_.toUpper(logging_clause)}` : ''}` +
            `${tablespace ? ` TABLESPACE ${tablespace}` : ''}` + 
            `${index_compression ? ` ${index_compression}` : ''}`;
    };

    return {
        getIndexType, getIndexKeys, getIndexOptions
    };
};
