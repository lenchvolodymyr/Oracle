module.exports = ({
    _,
    commentIfDeactivated,
    assignTemplates,
    templates,
    getNamePrefixedWithSchemaName,
    wrapInQuotes,
}) => {
    const getPlainUdt = (udt, getColumnDefinition) => {
        const udtName = getNamePrefixedWithSchemaName(udt.name, udt.schemaName);
        switch (udt.type) {
            case 'object_udt':
                return assignTemplates(templates.createObjectType, {
                    name: wrapInQuotes(udtName),
                    properties: _.map(udt.properties, (prop) => getColumnDefinition(prop, templates.objectTypeColumnDefinition)).join(',\n\t'),
                });
            case 'collection_udt':
                const defaultSize = udt.mode === 'VARRAY' ? '(64)' : '';
                return assignTemplates(templates.createCollectionType, {
                    name: wrapInQuotes(udtName),
                    collectionType: udt.mode,
                    size: _.isNumber(udt.size) ? `(${udt.size})` : defaultSize,
                    datatype: `${udt.ofType}${udt.nullable ? '' : ' NOT NULL'}`,
                    notPersistable: `${udt.notPersistable ? ' NOT PERSISTABLE' : ''}`,
                });
            default:
                return '';
        }
    };

    const getUserDefinedType = (udt, columns) => {
        return commentIfDeactivated(getPlainUdt(udt, columns), {
            isActivated: udt.isActivated,
        });
    };

    return { getUserDefinedType };
};
