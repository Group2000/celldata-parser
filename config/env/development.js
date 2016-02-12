"use strict";
var config={};



config.logging={
	colour:true,
	timestamp:true
};
config.watch={
	folder:'data/',
	processed:'data/processed/'
};
config.amqp={
	server:'amqp://localhost',
	queue:'measurements-dev'
};

module.exports=config;