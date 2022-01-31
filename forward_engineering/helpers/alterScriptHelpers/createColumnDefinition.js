module.exports = _ => {
	const createColumnDefinition = data => {
		return Object.assign(
			{
				name: '',
				type: '',
				nullable: true,
				primaryKey: false,
				default: '',
				length: '',
				scale: '',
				precision: '',
				comment: '',
			},
			data,
		);
	};

	const isNullable = (parentSchema, propertyName) => {
		if (!Array.isArray(parentSchema.required)) {
			return true;
		}

		return !parentSchema.required.includes(propertyName);
	};

	const getLength = jsonSchema => {
		if (_.isNumber(jsonSchema.length)) {
			return jsonSchema.length;
		} else if (_.isNumber(jsonSchema.maxLength)) {
			return jsonSchema.maxLength;
		} else {
			return '';
		}
	};

	const getNumber = val => _.isNumber(val) ? val : '';

	const getObject = val => _.isPlainObject(val) && !_.isEmpty(val) ? val : '';

	const getType = jsonSchema => {
		if (jsonSchema.$ref) {
			return jsonSchema.$ref.split('/').pop();
		}

		return _.toUpper(jsonSchema.mode || jsonSchema.childType || jsonSchema.type);
	};

	const createColumnDefinitionBySchema = ({ name, jsonSchema, parentJsonSchema, ddlProvider, schemaData }) => {
		const columnDefinition = createColumnDefinition({
			name: name,
			type: getType(jsonSchema),
			ofType: jsonSchema.ofType,
			nullable: isNullable(parentJsonSchema, name),
			default: jsonSchema.default,
			primaryKey: jsonSchema.primaryKey,
			unique: jsonSchema.unique,
			length: getLength(jsonSchema),
			scale: getNumber(jsonSchema.scale),
			precision: getNumber(jsonSchema.precision),
			comment: jsonSchema.description,
			fractSecPrecision: getNumber(jsonSchema.fractSecPrecision),
            withTimeZone: getNumber(jsonSchema.withTimeZone),
            localTimeZone: getNumber(jsonSchema.localTimeZone),
            yearPrecision: getNumber(jsonSchema.yearPrecision),
            dayPrecision: getNumber(jsonSchema.dayPrecision),
            lengthSemantics: jsonSchema.lengthSemantics,
            encryption: getObject(jsonSchema.encryption),
			isActivated: jsonSchema.isActivated,
		});

		return ddlProvider.hydrateColumn({
			columnDefinition,
			jsonSchema: {
				...jsonSchema,
				type: columnDefinition.type,
			},
			schemaData,
		});
	};

	return {
		createColumnDefinitionBySchema,
	};
};
