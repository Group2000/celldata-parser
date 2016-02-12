'use strict';

var _ = require('lodash');
var env = process.env.NODE_ENV || 'development'
/**
 * Load environment configuration
 */
module.exports = _.merge(
    require('./env/all.js'),
    require('./env/' + env + '.js') || {});
