var fdb = require('fdb').apiVersion(100);
var Readable = require('stream').Readable;

var ot = require('./ot');

// Export the memory store as livedb.memory
exports.memory = require('./memory');

var Livedb = exports.client = function(options) {
  if (!(this instanceof Livedb)) return new Livedb(options);

  if (options.fdb) {
    this.fdb = options.fdb;
  } else {
    try {
      // This raises a client_invalid_operation error if another process has
      // initialized fdb.
      fdb.init();
    } catch (e) {}

    this.fdb = fdb.open(options.fdbClusterFile, options.fdbName);

    // The number of watches effects how many documents we can have open
    // simultaneously.
    this.fdb.options.setMaxWatches(100000);
  }
  
  // Database which stores the documents.
  this.snapshotDb = options.snapshotDb || options.db || options;

  if (!this.snapshotDb.getSnapshot || !this.snapshotDb.writeSnapshot) {
    throw new Error('Missing or invalid snapshot db');
  }

  this.fdbPrefix = options.fdbPrefix || '/livedb';
  this.fdbPad = options.fdbPad || 7;

  var self = this;
};

Livedb.prototype.destroy = function() {
};

function pad(number, pad) {
  var N = Math.pow(10, pad);
  return number < N ? ("" + (N + number)).slice(1) : "" + number
};

// Get the key for an operation. Version is optional - if not specified we
// return the key at the end of the op range.
Livedb.prototype._getOpKey = function(cName, docName, v) {
  var key = this.fdbPrefix + '/' +
    cName + '/' +
    docName + '/' +
    ((v == null) ? '~' : pad(v, this.fdbPad));

  return key;
};

var parseKey = function(key) {
  //console.log(key, key.toString());
  var parts = key.toString().split('/');

  // Parts could contain junk at the start as a result of the fdbPrefix. We
  // only care about the end.
  var len = parts.length;

  var result = parts.slice(-3);
  if (result[result.length - 1] === '~') result.pop();

  return result;
};

var processOps = function(v, ops) {
  for (var i = 0; i < ops.length; i++) {
    ops[i] = JSON.parse(ops[i].value.toString());

    // Could also check for consistency here by parsing the version out of
    // the key and making sure it matches.
    ops[i].v = v++;
  }
};

// Non inclusive - gets ops from [from, to). Ie, all relevant ops. If to is
// not defined (null or undefined) then it returns all ops.
Livedb.prototype.getOps = function(cName, docName, from, to, callback) {
  // to is optional.
  if (typeof to === 'function') {
    callback = to;
    to = null;
  }
  
  if (from == null) return callback('Invalid from field in getOps');

  if (to != null && to >= 0 && from > to) return callback(null, []);

  // Ops are stored in '/livedb/ops/<cname>/<docname>/<v>
  var fromKey = this._getOpKey(cName, docName, from);

  var toKey = (to == null)
    // The fdb range is open, so whichever key this maps to won't appear in the results.
    ? fdb.KeySelector.firstGreaterThan(this._getOpKey(cName, docName))
    : this._getOpKey(cName, docName, to);

  //console.log(fromKey, toKey);
  this.fdb.doTransaction(function(tr, callback) {
    // We don't actually need a transaction for this - there's no reason for
    // this read to fail if a new operation is appended to the end of the range
    // here, which will happen.
    var iter = tr.snapshot.getRange(fromKey, toKey);
    iter.toArray(callback);
  }, function(err, ops) {
    if (err) return callback(err);
    processOps(from, ops);
    callback(err, ops);
  });
};

Livedb.prototype._getVersion = function(tr, cName, docName, callback) {
  tr.getKey(fdb.KeySelector.lastLessThan(this._getOpKey(cName, docName)), function(err, key) {
    if (err) return callback(err);

    if (key == null || !key.length) return callback(null, 0);

    var parts = parseKey(key);
    
    // It might have found a key in a different document! Interpret that as 0.
    if (parts[0] !== cName || parts[1] !== docName) return callback(null, 0);

    callback(null, (parts[2]|0) + 1);
  });
};

var logEntryForData = function(opData) {
  // Only put the op itself and the op's id in the log. The version can be
  // inferred via the version field.
  var entry = {};

  if (opData.src) entry.src = opData.src;
  if (opData.seq) entry.seq = opData.seq;
  if (opData.op) {
    entry.op = opData.op;
  } else if(opData.del) {
    entry.del = opData.del;
  } else if (opData.create) {
    entry.create = opData.create;
  }
  entry.m = opData.m; // Metadata.
  return entry;
};

var doNothing = function() {};


// Submit an operation on the named collection/docname. opData should contain a
// {op:}, {create:} or {del:} field. It should probably contain a v: field (if
// it doesn't, it defaults to the current version).
//
// callback called with (err, version, ops, snapshot)
Livedb.prototype.submit = function(cName, docName, opData, options, callback) {
  // Just put something in the db.
  if (typeof options === 'function') {
    callback = options;
    options = {};
  } else if (!options) options = {};

  if (!callback) callback = doNothing;

  var err = ot.checkOpData(opData);
  if (err) return callback(err);

  ot.normalize(opData);

  var self = this;

  var transformedOps = [];

  this.fdb.doTransaction(function(tr, callback) {
    // First we'll get a decent snapshot.
    self.snapshotDb.getSnapshot(cName, docName, function(err, snapshot) {
      if (err) return callback(err);

      snapshot = snapshot || {v:0};
      if (snapshot.v == null) return callback('Invalid snapshot data');

      // Ops are stored in '/livedb/ops/<cname>/<docname>/<v>
      var from = opData.v != null ? Math.min(snapshot.v, opData.v) : snapshot.v;

      var fromKey = self._getOpKey(cName, docName, from);
      // The fdb range is open, so whichever key this maps to won't appear in the results.
      var toKey = fdb.KeySelector.firstGreaterThan(self._getOpKey(cName, docName))

      var opts = {streamingMode: fdb.streamingMode.want_all};
      var iter = tr.snapshot.getRange(fromKey, toKey, opts);
      iter.toArray(function(err, ops) {
        if (err) return callback(err);

        processOps(from, ops);

        var err;
        for (var i = 0; i < ops.length; i++) {
          var op = ops[i];

          if (snapshot.v === op.v) {
            err = ot.apply(snapshot, op);
            if (err) return callback(err);
          }

          if (opData.src && opData.src === op.src && opData.seq === op.seq) {
            // The op has already been submitted. There's a variety of ways
            // this can happen. Do not transform it by itself & submit again.

            return callback('Op already submitted');
            //return callback(null, snapshot);
          }

          // Bring both the op and the snapshot up to date.
          if (opData.v === op.v) {
            transformedOps.push(op);
            err = ot.transform(snapshot.type, opData, op);
            if (err) return callback(err);
          }
        }

        if (opData.v != null && opData.v !== snapshot.v)
          return callback('Invalid opData version');

        // Ok, now we can try to apply the op.
        err = ot.apply(snapshot, opData);
        if (err) {
          if (typeof err !== 'string' && !isError(err)) {
            console.warn('validation function must return falsy, string or an error object.');
            console.warn('Instead we got', err);
          }
          return callback(err);
        }

        // If that worked, we can accept the operation by writing it. We know
        // there aren't other ops that matter because of the transaction.
        
        // This is still a little inefficient for conflict-heavy sitations - it
        // might be more efficient to use two transactions, bringing the op &
        // snapshot up to date in the first transaction then committing it in a
        // second transaction. However, conflicts are actually very rare in
        // practice.

        var v = opData.v != null ? opData.v : snapshot.v - 1;
        var data = JSON.stringify(logEntryForData(opData));
        tr.set(self._getOpKey(cName, docName, v), data);
        callback(null, snapshot);
      });
    });
  }, function(err, snapshot) {
    if (err) return callback(err);

    var v = snapshot.v - 1;

    // Update the snapshot for queries
    self.snapshotDb.writeSnapshot(cName, docName, snapshot, function(err) {
      // Its sort of too late to error out - the op has been commited.
      callback(err, v, transformedOps, snapshot);
    });
  });
};


// Callback called with (err, op stream). v must be in the past or present. Behaviour
// with a future v is undefined. (Don't do that.)
Livedb.prototype.subscribe = function(cName, docName, v, callback) {
  callback = callback || doNothing;
  // First catch up to the current version, then go in a loop watching for new
  // operations appearing in the stream.

  // Cache & reuse these.
  var stream = new Readable({objectMode: true});

  var self = this;

  this.getOps(cName, docName, v, null, function(err, ops) {
    if (err) return callback(err);

    for (var i = 0; i < ops.length; i++) {
      stream.push(ops[i]);
      v++;
    }

    var open = true;
    var reading = false;
    var watch = null;
    stream._read = function() {
      if (reading) return;
      reading = true;

      var key = self._getOpKey(cName, docName, v);
      self.fdb.getAndWatch(key, function(err, result) {
        if (err) return stream.emit('error', err);

        if (result.value) {
          var data = JSON.parse(result.value);
          data.v = v++;
          result.watch.cancel();
          reading = false;
          if (stream.push(data)) stream._read();
        } else {
          watch = result.watch;
          watch(function(err) {
            if (!open) return;
            if (err) return stream.emit('error', err);
            watch = null;
            self.fdb.get(key, function(err, res) {
              if (err) return stream.emit('error', err);
              var data = JSON.parse(res.toString());
              data.v = v++;
              //console.log('get', data);
              reading = false;
              if (stream.push(data)) stream._read();
            });
          });
        }
        //console.log(result);
      });
    };

    // To be consistant with the real livedb.
    stream.destroy = function() {
      if (!open) return;
      open = false;
      if (watch) watch.cancel();
      watch = null;
      stream.push(null);
      stream.emit('close');
      stream.emit('end');
    };

    callback(null, stream);
  });
};

// Requests is a map from {cName:{doc1:version, doc2:version, doc3:version}, ...}
Livedb.prototype.bulkSubscribe = function(requests, callback) {
  throw Error('Not implemented');
};

// Callback called with (err, {v, data})
Livedb.prototype.fetch = function(cName, docName, callback) {
  var self = this;

  this.snapshotDb.getSnapshot(cName, docName, function(err, snapshot) {
    if (err) return callback(err);

    snapshot = snapshot || {v:0};
    if (snapshot.v == null) return callback('Invalid snapshot data');

    self.getOps(cName, docName, snapshot.v, function(err, results) {
      if (err) return callback(err);

      if (results.length) {
        for (var i = 0; i < results.length; i++) {
          err = ot.apply(snapshot, results[i]);
          if (err) return callback(err);
        }
      }

      callback(null, snapshot);
    });
  });
};

// requests is a map from collection name -> list of documents to fetch. The
// callback is called with a map from collection name -> map from docName ->
// data.
//
// I'm not getting ops in redis here for all documents - I certainly could.
// But I don't think it buys us anything in terms of concurrency for the extra
// redis calls.
Livedb.prototype.bulkFetch = function(requests, callback) {
  throw Error('Not implemented');
};

Livedb.prototype.fetchAndSubscribe = function(cName, docName, callback) {
  var self = this;
  this.fetch(cName, docName, function(err, data) {
    if (err) return callback(err);
    self.subscribe(cName, docName, data.v, function(err, stream) {
      callback(err, data, stream);
    });
  });
};

Livedb.prototype.queryFetch = function(cName, query, opts, callback) {
  throw Error('Not implemented');
};

Livedb.prototype.query = function(index, query, opts, callback) {
  throw Error('Not implemented');
};


Livedb.prototype.collection = function(cName) {
  return {
    submit: this.submit.bind(this, cName),
    subscribe: this.subscribe.bind(this, cName),
    getOps: this.getOps.bind(this, cName),
    fetch: this.fetch.bind(this, cName),
    //fetchAndObserve: this.fetchAndObserve.bind(this, cName),
    queryFetch: this.queryFetch.bind(this, cName),
    query: this.query.bind(this, cName),
  };
};

