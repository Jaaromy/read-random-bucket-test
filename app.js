const aws = require('aws-sdk');
const Promise = require('bluebird');
aws.config.setPromisesDependency(Promise);
const s3 = new aws.S3();

function rand(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomBucket() {
	return s3.listBuckets().promise()
		.then(data => data.Buckets)
		.map(data => data.Name)
		.then(data => {
			return data.filter((item) => {
				return item.indexOf('trail') === -1;
			});
		})
		.then(filtered => {
			return filtered[rand(0, filtered.length - 1)]
		});
}

const getAllKeys = async (bucketName) => {
	// Prefixes are used to fetch data in parallel.
	const numbers = '0123456789'.split('');
	const letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
	const special = "!-_'.*()".split(''); // "Safe" S3 special chars
	const prefixes = [...numbers, ...letters, ...special];

	// array of params for each listObjectsV2 call
	const arrayOfParams = prefixes.map((prefix) => {
		return {
			Bucket: bucketName,
			Prefix: prefix
		}
	});

	const allKeys = [];
	await Promise.all(arrayOfParams.map(params => getKeys(params, allKeys)));
	return {
		bucket: bucketName,
		keys: allKeys
	};
};

async function getKeys(params, keys) {
	const response = await s3.listObjectsV2(params).promise();
	response.Contents.forEach(obj => keys.push(obj.Key));

	if (response.IsTruncated) {
		const newParams = Object.assign({}, params);
		newParams.ContinuationToken = response.NextContinuationToken;
		await getKeys(newParams, keys); // RECURSIVE CALL
	}
}

randomBucket()
	.then(r => getAllKeys(r))
	.then(obj => {
		console.log(JSON.stringify(obj));
	});
