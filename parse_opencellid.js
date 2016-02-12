var liner = require('./liner');
var opencellid = require('./opencellid');

process.stdin.pipe(liner).pipe(opencellid);
