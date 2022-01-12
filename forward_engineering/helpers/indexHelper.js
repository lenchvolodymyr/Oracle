module.exports = ({ _ }) => {
    const getIndexType = (indexType) => {
        return ` ${_.toUpper(indexType)}`;
    };

    const getIndexKeys = ({
        indxKey,
        column_expression,
    }) => {
        if (_.isArray(indxKey) && !_.isEmpty(indxKey)) {
            return `\n(\n\t${_.map(indxKey, ({name, type}) => `${name} ${_.toUpper(type)}`).join(',\n\t')}\n)\n`;
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
        }
        return `${logging_clause ? ` ${_.toUpper(logging_clause)}` : ''}` +
            `${tablespace ? ` TABLESPACE ${tablespace}` : ''}` + 
            `${index_compression ? ` ${index_compression}` : ''}` +
            `${index_attributes ? ` ${index_attributes}` : ''}`;
    };

    return {
        getIndexType, getIndexKeys, getIndexOptions
    };
};
