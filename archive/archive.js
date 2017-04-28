var request = require('request');

var SOURCE = 'https://willholley.cloudant.com/sourcetest';
var TARGET = 'https://willholley.cloudant.com/targettest';

var BATCH_SIZE = 500;
var SELECTOR = '';
var START_SEQ=0;

// while no more changes

// read batch_size docs from _changes, with selector
// POST _changes?filter=_selector&include_docs=true
// {"selector": SELECTOR}

// for each doc
// POST batch to target
// check all succeed
// POST delete to source for each


// track progress/output, last seq processed
var changesUrl = SOURCE + '_changes?filter=_selector&include_docs=true';

while(true) {
	request.post(changesUrl)
}



