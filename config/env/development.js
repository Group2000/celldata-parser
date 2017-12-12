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
	server:'amqp://user:password@rabbitmq',
	queue:'measurements-dev'
};

module.exports=config;