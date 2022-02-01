const { getAddContainerScript, getDeleteContainerScript } = require('./alterScriptHelpers/alterContainerHelper');
const {
	getAddCollectionScript,
	getDeleteCollectionScript,
	getAddColumnScript,
	getDeleteColumnScript,
	getModifyColumnScript,
} = require('./alterScriptHelpers/alterEntityHelper');
const {
	getDeleteUdtScript,
	getCreateUdtScript,
	getAddColumnToTypeScript,
	getDeleteColumnFromTypeScript,
	getModifyColumnOfTypeScript,
} = require('./alterScriptHelpers/alterUdtHelper');
const { getAddViewScript, getDeleteViewScript } = require('./alterScriptHelpers/alterViewHelper');

const getAlterContainersScripts = collection => {
	const addedContainers = collection.properties?.containers?.properties?.added?.items;
	const deletedContainers = collection.properties?.containers?.properties?.deleted?.items;

	const addContainersScripts = []
		.concat(addedContainers)
		.filter(Boolean)
		.map(container => getAddContainerScript(Object.keys(container.properties)[0]));
	const deleteContainersScripts = []
		.concat(deletedContainers)
		.filter(Boolean)
		.map(container => getDeleteContainerScript(Object.keys(container.properties)[0]));

	return [].concat(addContainersScripts).concat(deleteContainersScripts);
};

const getAlterCollectionsScripts = (collection, app, dbVersion) => {
	const createCollectionsScripts = []
		.concat(collection.properties?.entities?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => collection.compMod?.created)
		.map(getAddCollectionScript(app, dbVersion));
	const deleteCollectionScripts = []
		.concat(collection.properties?.entities?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => collection.compMod?.deleted)
		.map(getDeleteCollectionScript(app));
	const addColumnScripts = []
		.concat(collection.properties?.entities?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => !collection.compMod)
		.flatMap(getAddColumnScript(app));
	const deleteColumnScripts = []
		.concat(collection.properties?.entities?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => !collection.compMod)
		.flatMap(getDeleteColumnScript(app));
	const modifyColumnScript = []
		.concat(collection.properties?.entities?.properties?.modified?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => !collection.compMod)
		.flatMap(getModifyColumnScript(app));

	return [
		...createCollectionsScripts,
		...deleteCollectionScripts,
		...addColumnScripts,
		...deleteColumnScripts,
		...modifyColumnScript,
	].map(script => script.trim());
	return [];
};

const getAlterViewScripts = (collection, app) => {
	const createViewsScripts = []
		.concat(collection.properties?.views?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(view => ({ ...view, ...(view.role || {}) }))
		.filter(view => view.compMod?.created && view.selectStatement)
		.map(getAddViewScript(app));

	const deleteViewsScripts = []
		.concat(collection.properties?.views?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(view => ({ ...view, ...(view.role || {}) }))
		.map(getDeleteViewScript(app));

	return [...deleteViewsScripts, ...createViewsScripts].map(script => script.trim());
};

const getAlterModelDefinitionsScripts = (collection, app, dbVersion) => {
	const createUdtScripts = []
		.concat(collection.properties?.modelDefinitions?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(item => ({ ...item, ...(app.require('lodash').omit(item.role, 'properties') || {}) }))
		.filter(item => item.compMod?.created)
		.map(getCreateUdtScript(app, dbVersion));
	const deleteUdtScripts = []
		.concat(collection.properties?.modelDefinitions?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(item => ({ ...item, ...(app.require('lodash').omit(item.role, 'properties') || {}) }))
		.filter(collection => collection.compMod?.deleted)
		.map(getDeleteUdtScript(app));
	const addColumnScripts = []
		.concat(collection.properties?.modelDefinitions?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(item => !item.compMod)
		.map(item => ({ ...item, ...(app.require('lodash').omit(item.role, 'properties') || {}) }))
		.filter(item => item.childType === 'object_udt')
		.flatMap(getAddColumnToTypeScript(app));
	const deleteColumnScripts = []
		.concat(collection.properties?.modelDefinitions?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(item => !item.compMod)
		.map(item => ({ ...item, ...(app.require('lodash').omit(item.role, 'properties') || {}) }))
		.filter(item => item.childType === 'object_udt')
		.flatMap(getDeleteColumnFromTypeScript(app));

	const modifyColumnScripts = []
		.concat(collection.properties?.modelDefinitions?.properties?.modified?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(item => !item.compMod)
		.map(item => ({ ...item, ...(app.require('lodash').omit(item.role, 'properties') || {}) }))
		.filter(item => item.childType === 'object_udt')
		.flatMap(getModifyColumnOfTypeScript(app));

	return [
		...deleteUdtScripts,
		...createUdtScripts,
		...addColumnScripts,
		...deleteColumnScripts,
		...modifyColumnScripts,
	].map(script => script.trim());
};

module.exports = {
	getAlterContainersScripts,
	getAlterCollectionsScripts,
	getAlterViewScripts,
	getAlterModelDefinitionsScripts,
};
