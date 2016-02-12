//production Configuration
"use strict";
var config={};

config.logging={
	colour:false,
	timestamp:false
};
config.watch={
	folder:'data/',
	processed:'data/processed/'
};
config.amqp={
	server:'amqp://localhost',
	queue:'measurements'
};

module.exports=config;