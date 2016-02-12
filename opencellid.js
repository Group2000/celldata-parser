var stream = require('stream');
var util = require('util');
var opencellid = new stream.Transform({objectMode: true});

opencellid.gps = null;
opencellid.neighbors = [];
opencellid.measurements = [];

opencellid._transform = function(chunk, encoding, callback) {
  var row = chunk.toString().split(',');

  var measurement = {};
  measurement.source = 'opencellid';
  measurement.mcc = parseInt(row[0]);
  measurement.net = parseInt(row[1]);
  measurement.area = parseInt(row[2]);
  measurement.cell = parseInt(row[3]);
  measurement.location = [parseFloat(row[4]), parseFloat(row[5])];
  if (row[6])
    measurement.signal = parseFloat(row[6]);
  measurement.timestamp = parseInt(row[7]);
  if (row[10])
    measurement.speed = parseFloat(row[10]);
  if (row[11])
  	measurement.direction = parseFloat(row[11]);
  measurement.radio = row[12];
  if (row[16])
    measurement.unit = parseInt(row[16]);
  if (row[18])
    measurement.unit = parseInt(row[18]);
  
  this.push(measurement);
  callback();
}

module.exports = opencellid
