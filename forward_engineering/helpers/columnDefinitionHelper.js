module.exports = ({
    _,
    wrap,
    assignTemplates,
    templates,
    commentIfDeactivated,
    wrapComment,
    wrapInQuotes
}) => {

    const getColumnComments = (tableName, columnDefinitions) => {
        return _.chain(columnDefinitions)
            .filter('comment')
            .map(columnData => {
                const comment = assignTemplates(templates.comment, {
                    object: 'COLUMN',
                    objectName: `${tableName}.${wrapInQuotes(columnData.name)}`,
                    comment: wrapComment(columnData.comment),
                });

                return commentIfDeactivated(comment, columnData);
            })
            .join('\n')
            .value();
    };

    const getColumnConstraints = ({nullable, unique, primaryKey, primaryKeyOptions, uniqueKeyOptions}) => {
        const getOptionsString = ({
            deferClause,
			rely,
			validate,
			indexClause,
			exceptionClause,
        }) => `${deferClause ? ` ${deferClause}` : ''}${rely ? ` ${rely}` : ''}${indexClause ? ` ${indexClause}` : ''}${validate ? ` ${validate}` : ''}${exceptionClause ? ` ${exceptionClause}` : ''}`;
        const primaryKeyString = primaryKey ? ` PRIMARY KEY${getOptionsString(primaryKeyOptions || {})}` : '';
        const uniqueKeyString = unique ? ` UNIQUE${getOptionsString(uniqueKeyOptions || {})}` : '';
        return `${nullable ? '' : ' NOT NULL'}${primaryKeyString}${uniqueKeyString}`;
    };

    const replaceTypeByVersion = (type, version) => {
        if (type === 'JSON' && version !== '21c') {
            return 'CLOB';
        }
        return type;
    };

    const getColumnDefault = ({default: defaultValue}) => {
        if (defaultValue) {
            return ` DEFAULT ${defaultValue}`;
        }
        return '';
    };

    const getColumnEncrypt = ({encryption}) => {
        if (encryption && !_.isEmpty(encryption)) {
            const {
                ENCRYPTION_ALGORITHM,
                INTEGRITY_ALGORITHM,
                noSalt,
            } = encryption;
            return ` ENCRYPT${ENCRYPTION_ALGORITHM ? ` USING ${ENCRYPTION_ALGORITHM}` : ''}${INTEGRITY_ALGORITHM ? ` ${wrapInQuotes(INTEGRITY_ALGORITHM)}` : ''}${noSalt ? ' NO SALT' : ''}`;
        }
        return '';
    };

    const addByteLength = (type, length, lengthSemantics) => {
        return ` ${type}(${length} ${_.toUpper(lengthSemantics)})`;
    }

    const addLength = (type, length) => {
        return ` ${type}(${length})`;
    };

    const addScalePrecision = (type, precision, scale) => {
        if (_.isNumber(scale)) {
            return ` ${type}(${precision ? precision : '*'},${scale})`;
        } else {
            return ` ${type}(${precision})`;
        }
    };

    const addPrecision = (type, precision) => {
        if (_.isNumber(precision)) {
            return ` ${type}(${precision})`;
        }
        return type;
    };

    const timestamp = (fractSecPrecision, withTimeZone, localTimeZone) => {
        return ` TIMESTAMP ${_.isNumber(fractSecPrecision) ? `(${fractSecPrecision})` : ''} WITH ${localTimeZone ? 'LOCAL' : ''} TIME ZONE`;;
    };

    const intervalYear = (yearPrecision) => {
        return ` INTERVAL YEAR ${_.isNumber(yearPrecision) ? `(${yearPrecision})` : ''} TO MONTH`;
    };

    const intervalDay = (dayPrecision, fractSecPrecision) => {
        return ` INTERVAL DAY ${_.isNumber(dayPrecision) ? `(${dayPrecision})` : ''} TO SECOND ${_.isNumber(fractSecPrecision) ? `(${fractSecPrecision})` : ''}`;
    };

    const canHaveByte = type => ['CHAR', 'VARCHAR2'].includes(type);
    const canHaveLength = type => ['CHAR', 'VARCHAR2', 'NCHAR', 'NVARCHAR2', 'RAW', 'UROWID'].includes(type);
    const canHavePrecision = type => ['NUMBER', 'FLOAT'].includes(type);
    const canHaveScale = type => type === 'NUMBER';
    const isIntervalYear = type => type === 'INTERVAL YEAR';
    const isIntervalDay = type => type === 'INTERVAL DAY';
    const isTimezone = type => type === 'TIMESTAMP';

    const decorateType = (type, columnDefinition) => {
        switch(true) {
            case (canHaveByte(type) && canHaveLength(type) && _.isNumber(columnDefinition.length) && columnDefinition.lengthSemantics):
                return addByteLength(type, columnDefinition.length, columnDefinition.lengthSemantics);
            case (canHaveLength(type) && _.isNumber(columnDefinition.length)):
                return addLength(type, columnDefinition.length);
            case (canHavePrecision(type) && canHaveScale(type) && _.isNumber(columnDefinition.precision)):
                return addScalePrecision(type, columnDefinition.precision, columnDefinition.scale);
            case (canHavePrecision(type) && _.isNumber(columnDefinition.precision)):
                return addPrecision(type, columnDefinition.precision);
            case (isTimezone(type)):
                return timestamp(columnDefinition.fractSecPrecision ,columnDefinition.withTimeZone, columnDefinition.localTimeZone);
            case (isIntervalYear(type)):
                return intervalYear(columnDefinition.yearPrecision);
            case (isIntervalDay(type)):
                return intervalDay(columnDefinition.dayPrecision, columnDefinition.fractSecPrecision);
            default:
                return type;
        }
    }; 

    return {
        getColumnComments,
        getColumnConstraints,
        replaceTypeByVersion,
        getColumnDefault,
        getColumnEncrypt,
        decorateType,
    };
};