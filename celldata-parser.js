var config = require('./config/config');
var amqp = require('amqplib');
var fs = require('fs');
var path = require('path');
var celllog = require('./celllog');
var logger = require('./utils/logger');
var zlib = require('zlib');
var i_cell = 0;
var i_wifi = 0;
var i = 0;
var split = require('split');

var watchFolder = __dirname + '/' + config.watch.folder;
var processedFolder = __dirname + '/' + config.watch.processed;
var files = [];
var channel;

function mkCallback(i) {

	return function(err) {
		if (err !== null) {
			logger.log('error', 'Message' + i + 'failed');
		}
	}
}

function processFile(filename, ch) {
	var readStream = fs.createReadStream(watchFolder + filename);

	readStream.on('open', function() {
		logger.log('info', 'Open file: ' + filename);
		clparser = celllog.createCellog();

		var cl = readStream.pipe(split()).pipe(clparser);

		cl.on('data', function(chunk) {
			i++;
			var send = JSON.stringify(chunk);
			if (chunk.bssid) {
				i_wifi++;
				ch.publish(config.amqp.queue, 'WIFI', new Buffer(send), {},
						mkCallback(i));
			} else {
				i_cell++;
				ch.publish(config.amqp.queue, 'cell', new Buffer(send), {},
						mkCallback(i));
			}

		});
		cl.on('end', function() {
			logger.log('status', 'FINISHED PROCESSING ' + filename);
			processNext();
		});

	});

	readStream.on('end', function() {
		logger.log('info', 'Messages processed:' + i + ' total (' + i_cell
				+ ' cells, ' + i_wifi + ' wifi)');

		fs.rename(watchFolder + filename, processedFolder + filename, function(
				err) {
			if (err)
				console.log(err);

		});
	})
}

function processZippedFile(filename, ch) {
	logger.log('status', 'Unzipping file ' + filename);
	const
	gunzip = zlib.createGunzip();
	const
	fs = require('fs');
	const
	inp = fs.createReadStream(watchFolder + filename);
	clparser = celllog.createCellog();

	var cl = inp.pipe(gunzip).pipe(split()).pipe(clparser);

	cl.on('data', function(chunk) {
		i++;
		var send = JSON.stringify(chunk);
		if (chunk.bssid) {
			i_wifi++;
			ch.publish(config.amqp.queue, 'WIFI', new Buffer(send), {},
					mkCallback(i));
		} else {
			i_cell++;
			ch.publish(config.amqp.queue, 'cell', new Buffer(send), {},
					mkCallback(i));
		}

	});
	cl.on('end', function() {
		logger.log('status', 'FINISHED PROCESSING ' + filename);
		logger.log('info', 'Messages processed:' + i + ' total (' + i_cell
				+ ' cells, ' + i_wifi + ' wifi)');

		fs.rename(watchFolder + filename, processedFolder + filename, function(
				err) {
			if (err) {
				console.log(err);
			}
		});
		processNext();
	});
}

amqp.connect(config.amqp.server).then(function(c) {
	c.createConfirmChannel().then(function(ch) {
		channel = ch;

		logger.log('status', 'AMQP connection established');
		// read current files in dir
		fs.readdir(watchFolder, function(err, newFiles) {
			files = files.concat(newFiles);
		});

		// start watching dir for new files
		logger.log('status', 'Watching folder ' + watchFolder);
		fs.watch(watchFolder, function(event, filename) {
			if (filename) {
				if (event === 'rename') {
					files.push(filename);
				}
			}
		});
		setTimeout(processNext, 1000);
	});
});

function processNext() {
	var file = files.pop();
	logger.log('status', 'processNext file: ' + file);
	if (typeof file !== 'undefined' && file) {
		fs.stat(watchFolder + file, function(err, stats) {
			if (err) {
				// File does not exist
				processNext();
			} else if (stats.isFile() && path.extname(file) === '.log') {
				processFile(file, channel);
			} else if (stats.isFile() && path.extname(file) === '.gz') {
				processZippedFile(file, channel);
			} else {
				processNext();
			}
		});
	} else {
		setTimeout(processNext, 10000);
	}
}
