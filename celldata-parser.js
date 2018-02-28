var config = require('./config/config');
var amqp = require('amqplib/callback_api');
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
var amqpConnection;
var channel;


// method that starts this processed
// 0. connect to RabbitMQ
// 1. reads current
// 2. start watch folder
// 3. start processNext method
function startup() {
	connectToRabbitMQ();
	logger.log('status', 'CellData parser is started');

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

}

function mkCallback(i) {

	return function(err) {
		if (err !== null) {
			logger.log('error', 'Message' + i + 'failed');
		}
	}

}

// processFile opens the given filename, split it, parse it and send to
// the WIFI or cell queue.
function processFile(filename) {
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
				sendToQueue('WIFI', send);
			} else {
				i_cell++;
				sendToQueue('cell', send);
			}

		});
		cl.on('end', function() {
			logger.log('status', 'FINISHED PROCESSING ' + filename);
			processNext();
		});

	});

	readStream.on('end', function() {
		logger.log('info', 'Messages processed:' + i + ' total (' + i_cell + ' cells, ' + i_wifi + ' wifi)');

		fs.rename(watchFolder + filename, processedFolder + filename, function(err) {
			if (err) {
				logger.log(err);
			}
		});
	})
}

function processZippedFile(filename) {
	logger.log('status', 'Unzipping file ' + filename);
	const gunzip = zlib.createGunzip();
	const inp = fs.createReadStream(watchFolder + filename);

	clparser = celllog.createCellog();

	var cl = inp.pipe(gunzip).pipe(split()).pipe(clparser);

	cl.on('data', function(chunk) {
		i++;
		var send = JSON.stringify(chunk);
		if (chunk.bssid) {
			i_wifi++;
			sendToQueue('WIFI', send);
		} else {
			i_cell++;
			sendToQueue('cell', send);
		}

	});
	cl.on('end', function() {
		logger.log('status', 'FINISHED PROCESSING ' + filename);
		logger.log('info', 'Messages processed:' + i + ' total (' + i_cell + ' cells, ' + i_wifi + ' wifi)');

		fs.rename(watchFolder + filename, processedFolder + filename, function(err) {
			if (err) {
				logger.log(err);
			}
		});
		processNext();
	});
}

function connectToRabbitMQ() {
	logger.log('status', "[AMQP] start connect");
	amqp.connect(config.amqp.server, function(err, conn) {
		if (err) {
      logger.log('error', "[AMQP] couldn't connect to RabbitMQ " + err.message);
			// retry in 1 second
      return setTimeout(connectToRabbitMQ, 1000);
    }
		conn.on("error", function(err) {
			if (err.message !== "Connection closing") {
				 logger.log('error', "[AMQP] conn error " + err.message);
			}
	 	});
	 	conn.on("close", function() {
			 logger.log('error', "[AMQP] reconnecting");
			 // retry in 1 second
			 return setTimeout(connectToRabbitMQ, 1000);
	 	});
	 	logger.log('status', "[AMQP] connected");
	 	amqpConnection = conn;

		createConfirmChannel();
	});
}

// closing connection when there is an error
function closeOnErr(err) {
  if (!err) return false;
  logger.log('error', "[AMQP] error " + err);
  amqpConnection.close();
  return true;
}

function createConfirmChannel() {
	amqpConnection.createConfirmChannel(function(err, ch) {
		if (closeOnErr(err)) return;
    ch.on("error", function(err) {
      logger.log('error', "[AMQP] channel error", err.message);
    });
    ch.on("close", function() {
      logger.log('status', "[AMQP] channel closed");
			setTimeout(createConfirmChannel, 1000);
    });
		channel = ch;
		logger.log('status', "[AMQP] channel created");
		setTimeout(processNext, 1000);
	});
}

// send data to queue name, makes a connection first and closes it afterwards.
function sendToQueue(routingKey, data) {
	channel.publish(config.amqp.queue, routingKey, new Buffer(data), {persistent: true}, mkCallback(i));
}

function processNext() {
	var file = files.pop();
	if (typeof file !== 'undefined' && file) {
		logger.log('status', 'processNext file: ' + file);
		fs.stat(watchFolder + file, function(err, stats) {
			if (err) {
				// File does not exist
				processNext();
			} else if (stats.isFile() && path.extname(file) === '.log') {
				processFile(file);
			} else if (stats.isFile() && path.extname(file) === '.gz') {
				processZippedFile(file);
			} else {
				processNext();
			}
		});
	} else {
		setTimeout(processNext, 10000);
	}
}

// End yes we're up and running
startup();
