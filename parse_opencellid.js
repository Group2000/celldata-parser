var config = require('./config/config');
var amqp = require('amqplib');
var fs=require('fs');
var path=require('path');
var logger = require('./utils/logger');
var liner = require('./liner');
var opencellid = require('./opencellid');

var filename = process.argv[2];

var i_cell=0;
var i=0;
function mkCallback(i) {

	return function(err) {
		if (err !== null) {
			logger.log('error', 'Message' + i + 'failed');
		}
	}
}

function processFile(filename, ch) {
	var readStream = fs.createReadStream(filename);

	readStream.on('open', function() {
		logger.log('info', 'Open file: ' + filename);
		var split = require('split');

		var cl = readStream.pipe(split()).pipe(opencellid);

		cl.on('data', function(chunk) {
			i++;
			var send = JSON.stringify(chunk);

			i_cell++;
			ch.publish(config.amqp.queue, 'cell', new Buffer(send), {},
					mkCallback(i));

		});
		cl.on('end', function() {
			logger.log('status', 'FINISHED PROCESSING ' + filename);
		});

	});

	readStream.on('end', function() {
		logger.log('info', 'Messages processed:' + i + ' total (' + i_cell
				+ ' cells, ' + i_wifi + ' wifi)');
	})
}

amqp.connect(config.amqp.server).then(function(c) {
	c.createConfirmChannel().then(function(ch) {

		logger.log('status', 'AMQP connection established');
		processFile(filename, ch);
	});
});
