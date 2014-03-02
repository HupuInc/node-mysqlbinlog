"use strict";
this.MysqlBinlogTailer = MysqlBinlogTailer;

var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var path = require('path');

/**
 * Tails a Mysql binlog and emits an event for every query executed.
 */
function MysqlBinlogTailer(index) {
	EventEmitter.call(this);
	this.path = path.dirname(index);
	fs.readFile(index, 'utf-8', function(err, data) {
		if (err) {
			return this.emit('error', err);
		}
		var index = data.split('\n');
		var latest;
		do {
			latest = index.pop();
		} while(latest === '');
		if (!latest) {
			return this.emit('error', new Error('No binlogs?'));
		}
		this.stream(path.join(this.path, latest), true);
	}.bind(this));
}
MysqlBinlogTailer.prototype = Object.create(EventEmitter.prototype);

MysqlBinlogTailer.prototype.stream = function(file, fastForward) {
	var that = this, fastForwardUntil;

	//
	// Binlog parsers

	// Read binlog header
	function expectHeader() {
		read(4, function(buffer) {
			// Magic number
			if (buffer[0] === 0xfe && buffer[1] === 0x62 && buffer[2] === 0x69 && buffer[3] === 0x6e) {
				expectFirstEvent();
			} else {
				stop();
				return that.emit('error', new Error('Invalid binlog'));
			}
		});
	}

	// Get first event, with version information
	function expectFirstEvent() {
		read(19, function(buffer) {
			var ts = readTimestamp(buffer, 0);
			if (buffer[4] === 15) { // FORMAT_DESCRIPTION_EVENT
				skip(readInt(buffer, 9) - 19);
				that.emit('log', ts, file);
				expectEvent();
			} else {
				stop();
				return that.emit('error', new Error('Invalid binlog version'));
			}
		});
	}

	// Parse an event
	function expectEvent() {
		read(19, function(buffer) {
			switch (buffer[4]) {
				case 2: // QUERY_EVENT
					parseQueryEvent(buffer);
					break;
				case 3: // STOP_EVENT
					parseStopEvent(buffer);
					break;
				case 4: // ROTATE_EVENT
					parseRotateEvent(buffer);
					break;
				case 5: // INTVAR_EVENT
					parseAutoInt(buffer);
					break;
				default:
					skip(readInt(buffer, 9) - 19);
					break;
			}
		});
	}

	// Parse a query event
	var lastInsertId = undefined, autoIncrement = undefined;
	function parseQueryEvent(buffer) {
		var ts = readTimestamp(buffer, 0);
		var eventLength = readInt(buffer, 9);

		// Are we fast-forwarding? If so skip the query
		if (fastForwardUntil > fileOffset) {
			skip(eventLength - buffer.length);
			expectEvent();
			return;
		}

		// Read query data
		read(eventLength - buffer.length, function(buffer) {
			var nameLen = buffer[8];
			var statusLen = readSmallint(buffer, 11);
			var meta;
			if (lastInsertId || autoIncrement) {
				meta = {};
				meta.lastInsertId = lastInsertId;
				meta.autoIncrement = autoIncrement;
				lastInsertId =
				autoIncrement = undefined;
			}
			var db = buffer.slice(13 + statusLen, 13 + statusLen + nameLen).toString('utf-8');
			var query = buffer.slice(14 + nameLen + statusLen).toString('utf-8');
			that.emit('query', ts, db, query, meta);
			expectEvent();
		});
	}

	// Parse an auto_increment event
	function parseAutoInt(buffer) {
		read(9, function(buffer) {
			if (buffer[0] === 1) { // LAST_INSERT_ID_EVENT
				lastInsertId = readInt(buffer, 1);
			} else if (buffer[0] === 2) { // INSERT_ID_EVENT
				autoIncrement = readInt(buffer, 1);
			}
			expectEvent();
		});
	}

	// Parse a rotation event; onto the next file!
	function parseRotateEvent(buffer) {
		var eventLength = readInt(buffer, 9);
		read(eventLength - buffer.length, function(buffer) {
			// Stop reading this log and wait for the next one
			stop();
			var next = path.join(that.path, buffer.slice(8).toString('utf-8'));
			that.emit('rotate', next);
			fs.watchFile(next, function(stat) {
				fs.unwatchFile(next);
				that.stream(next);
			});
		});
	}

	// Parse a stop event (which is empty)
	function parseStopEvent(buffer) {
		var ts = readTimestamp(buffer, 0);
		that.emit('stop', ts);
	}

	//
	// Utilities
	function readTimestamp(buffer, offset) {
		return new Date(readInt(buffer, offset) * 1000);
	}

	function readInt(buffer, offset) {
		return buffer[offset + 3] * 0x1000000 +
			buffer[offset + 2] * 0x10000 +
			buffer[offset + 1] * 0x100 +
			buffer[offset];
	}

	function readSmallint(buffer, offset) {
		return buffer[offset + 1] * 0x100 +
			buffer[offset];
	}

	//
	// File handling functions

	// Stat, open, and watch this file
	var stat, handle;
	fs.stat(file, function(err, val) {
		if (err) {
			return that.emit('error', err);
		}
		stat = val;
		if (fastForward) {
			fastForwardUntil = stat.size;
		}

		// Open the file
		fs.open(file, 'r', undefined, function(err, val) {
			if (err) {
				return that.emit('error', err);
			}
			handle = val;
			ready = true;
			readChunk();

			// And watch for changes
			fs.watchFile(file, function(val) {
				stat = val;
				readChunk();
			});
		});
	});

	// Requests a chunk of a certain size, doesn't return until that chunk is ready
	var didReadCallback, bytesWanted;
	function read(bytes, cb) {
		bytesWanted = bytes;
		didReadCallback = cb;
		readChunk();
	}

	// Skips forward in the file. Only affects next `read`, doesn't actually do anything to the file
	function skip(offset) {
		fileOffset += offset;
	}

	// Terminates all access to this file
	function stop() {
		fs.unwatchFile(file);
		fs.close(handle);
		ready = false;
	}

	// Read a chunk
	var fileOffset = 0, bytesWanted, ready = false;
	function readChunk() {
		if (!ready) {
			// Not ready to read?
			return;
		} else if (!bytesWanted) {
			// What?
			throw new Error('No bytes wanted?');
		} else if (stat.size < fileOffset + bytesWanted) {
			// Still waiting on data from file
			return;
		}

		// Good to read
		ready = false;
		fs.read(handle, new Buffer(bytesWanted), 0, bytesWanted, fileOffset, function(err, read, buffer) {
			if (err) {
				stop();
				return this.emit('error', err);
			}
			fileOffset += bytesWanted;
			bytesWanted = 0;
			ready = true;
			didReadCallback(buffer);
		});
	}

	// Begin!
	expectHeader();
};
