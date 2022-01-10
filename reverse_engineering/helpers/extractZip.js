const AdmZip = require('adm-zip');
const path = require('path');

const extractZip = (zipPath, destination) =>
	new Promise((resolve, reject) => {
		try {
			const zip = new AdmZip(zipPath);
	
			if (!zip) {
				return reject(
					new Error('Link to zip file is broken or system has no permissions to ' + zipPath + ' folder'),
				);
			}
	
			const zipEntries = zip.getEntries();
	
			if (Object.keys(zipEntries).length === 0) {
				return reject(new Error('Zip file is empty'));
			}
	
			zip.extractAllTo(destination, true);
	
			return resolve(destination);
		} catch (error) {
			reject(error);
		}
	});

module.exports = extractZip;
