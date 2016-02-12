
var logger=exports;
var colour = require('./colour');
var config=require('../config/config.js')
var coding = {
  log: 'black',
  info: 'yellow',
  status: 'green',
  fail: 'red',
  error: 'red',
};



logger.debugLevel='warn';
logger.log = function(level,message) {
	var levels=['error','warn','info','status','log'];
	if(levels.indexOf(level)>=levels.indexOf(logger.debuglevel)){
		if (typeof message!=='string'){
		
			message=JSON.stringify(message);
		}
		var d_string=new Date().toISOString().
			replace(/T/,' ').
			replace(/\..+/, '');
		
		message=' [' + config.service.name + '] '+ message; 
		if(config.logging.colour){
			message=colour(coding[level], message)
		}
		if(config.logging.timestamp){
			message=d_string +' - ' +message;
		}
		console.log(message)
	}
}