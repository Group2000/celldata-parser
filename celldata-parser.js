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
	logger.log('status','Unzipping file ' + filename);
	const gunzip = zlib.createGunzip();
	const fs = require('fs');
	const inp = fs.createReadStream(watchFolder+filename);
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
		
		fs.rename(watchFolder +filename,processedFolder+filename,function(err){
			if(err){
				console.log(err);
			}
		});
	});
}

amqp.connect(config.amqp.server).then(function(c) {
	c.createConfirmChannel().then(function(ch) {

		logger.log('status', 'AMQP connection established');
		// read current files in dir
		fs.readdir(watchFolder, function(err, files) {
			files.map(function(file) {
				fs.stat(watchFolder + file, function(err, stats) {
					if (stats.isFile() && path.extname(file) === '.log') {
						processFile(file, ch);
					}
				})
			})
		});

		// start watching dir for new files
		logger.log('status', 'Watching folder ' + watchFolder);
		fs.watch(watchFolder, function(event, filename) {

			if (filename) {
				if (event === 'rename') {
					if (path.extname(filename) === '.log') {
						fs.stat(watchFolder + filename, function(err, stats) {
							if (!err)
								if (stats.isFile()) {
									processFile(filename, ch);
								}
						});
					} else if (path.extname(filename) === '.gz') {
						fs.stat(watchFolder + filename, function(err, stats) {
							if (!err)
								if (stats.isFile()) {
									processZippedFile(filename, ch);
								}
						});
					}
				}
			}

		});

	});
});
