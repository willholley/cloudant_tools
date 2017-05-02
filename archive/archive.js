var PouchDB = require('pouchdb'),
	crypto = require('crypto'),
	shasum = crypto.createHash('sha1'),
	winston = require('winston'),
	config = require('config');

winston.level = process.env.LOG_LEVEL;

winston.log('info', 'starting archive');

var SOURCE = config.get('source');
var TARGET = config.get('target');
var SELECTOR = config.get('selector');
var BATCH_SIZE = config.get('batch_size');
var START_SEQ = config.get('start_seq');
var DRY_RUN = config.get('dry_run');

if (SOURCE.length === 0) {
	throw Error('no source database defined');
}

if (TARGET.length === 0) {
	throw Error('no target database defined');
}

if (!DRY_RUN) {
	winston.log('info', 'NOT DRY RUN!!!');
} else {
	winston.log('info', 'DRY RUN');
}

var source = new PouchDB(SOURCE);
var target = new PouchDB(TARGET);

var count = 0;

var archive_id = shasum.update(SOURCE + DRY_RUN).digest('hex');
var checkpointDoc = '_local/' + archive_id;


function writeCheckpoint (seq) {
	getCheckpoint().then(function (doc) {
		winston.log('verbose', 'writing checkpoint ' + checkpointDoc);
		doc.seq = seq;
		doc.count = count;
		return target.put(doc);
	});
}

function getCheckpoint () {
	winston.log('verbose', 'fetching checkpoint ' + checkpointDoc);
	return target.get(checkpointDoc).
		catch(function (err) {
			winston.log('verbose', 'checkpoint ' + checkpointDoc, err);

			if (err.status === 404) {
				return {
					'_id': checkpointDoc,
					'seq': 0,
					'count': 0
				};
			}
			winston.log('error', err);
		}).then(function (doc) {
			return doc;
		});
}

function writeBatch (seq, changes) {
	winston.log('debug', 'writeBatch', changes);
	winston.log('verbose', 'writeBatch');
	var lastSeq = changes.last_seq;
	var docs = changes.results.map(row => row.doc);

	if (DRY_RUN) {
		// non-destructive run
		count = count + docs.length;
		winston.log('info', 'DRY RUN: total docs moved: ' + count);

		getNextBatch(lastSeq);
		return;
	}

	target.bulkDocs(docs, {
		new_edits: false
	}).then(function (result) {
		winston.log('info', 'source sequence ' + lastSeq + ' written to target');

		deleteBatch(seq, lastSeq, docs);
	}).catch(function (err) {
		winston.log('error', 'FATAL: error writing batch for seq ' + seq + ':\n' + err);
	});
}

function deleteBatch (startSeq, lastSeq, docs) {
	winston.log('verbose', 'deleteBatch', {
		"startSeq": startSeq,
		"lastSeq": lastSeq
	});
	winston.log('debug', 'deleteBatch', {
		"docs": docs
	});

	var idRevPairs = docs.map(function(doc) {
		return {
			_id: doc._id,
			_rev: doc._rev,
			_deleted: true
		};
	});
	source.bulkDocs(idRevPairs).then(function (result) {
		count = count + idRevPairs.length;
		winston.log('info', 'source sequence ' + lastSeq + ' deleted from source');
		winston.log('info', 'total docs moved: ' + count);

		getNextBatch(lastSeq);
	}).catch(function (err) {
		winston.log('warn', 'error writing batch for start seq ' + startSeq + ':\n' + err);

		// ignore failed deletions for now
		getNextBatch(lastSeq);
	});
}


function getNextBatch(seq) {
	winston.log('verbose', 'getNextBatch', {
		"seq": seq,
		"selector": JSON.stringify(SELECTOR)
	});

	source.changes({
		include_docs: true,
		since: seq,
		limit: BATCH_SIZE,
		selector: SELECTOR
	}).on('complete', function(info) {
		if (info.results.length === 0) {
			winston.log('info', 'No more results at seq ' + seq);
			winston.log('info', 'All done!');
			return;
		}

		try {
			writeBatch(seq, info);
		} catch (e) {
			console.log(e);
		}

	}).on('error', function(err) {
		winston.log('error', err);
	});
}

if (START_SEQ === 0) {
	winston.log('info', 'no start seq provided');

	getCheckpoint().then(function (doc) {
		winston.log('info', 'using start_seq ' + doc.seq);
		count = doc.count;
		getNextBatch(doc.seq);
	});
}
else {
	winston.log('info', 'using start_seq ' + START_SEQ);
	getNextBatch(START_SEQ);
}
