var stream = require('stream');
var liner = new stream.Transform({objectMode: true});

liner._transform = function(chunk, encoding, callback) {
  var data = chunk.toString();
  if (this._lastLineData)
    data = this._lastLineData + data;

  var lines = data.split('\n');
  this._lastLineData = lines.splice(lines.length-1, 1)[0];

  lines.forEach(this.push.bind(this));
  callback();
}

liner._flush = function(callback) {
  if (this._lastLineData)
    this.push(this._lastLineData)
  this._lastLineData = null;
  callback();
}

module.exports = liner;
