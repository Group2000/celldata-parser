var stream = require('stream');
var util = require('util');
var logger = require('./utils/logger');
var celllog;

module.exports = {
  createCellog:function(){
    
    celllog=new stream.Transform({objectMode: true});
    celllog.gps = null;
    celllog.neighbors = [];
    celllog.measurements = [];

    celllog._transform = function(chunk, encoding, callback) {

      var row = chunk.toString().split(',');
      
      this.localtime = parseLocalDateTime(row[0]);
      switch(row[1]) {
        case 'GPS':
          this.gps = parseGPS(row);
          break;
        case 'GSM':
          var measurement = parseGSM(row, this.gps);
          if (measurement != null)
            this.push(measurement);
          break;
        case 'UMTS':

          var measurement = parseUMTS(row, this.gps, this.measurements, this.neighbors);
          if (measurement != null)
            this.push(measurement);
          break;
        case 'LTE':
          var measurement = parseLTE(row, this.gps,this.measurements, this.neighbors);
          if (measurement != null)
            this.push(measurement);
          break;
        case 'WIFI':

          var measurement=parseWIFI(row,this.gps,this.measurements, this.neighbors);
          if(measurement!=null)
            
            this.push(measurement);
          break;
            
      }
      callback();
    }

    celllog._flush = function(callback) {
      for (var i=0; i<this.neighbors.length; i++) {
        var neighbor = this.neighbors[i];
        for (var j=0; j<this.measurements.length; j++) {
          var measurement = this.measurements[j];
          if (neighbor.mcc == measurement.mcc &&
              neighbor.net == measurement.net &&
              neighbor.channel == measurement.channel &&
              neighbor.unit == measurement.unit &&
              Math.abs(neighbor.timestamp - measurement.timestamp) < 3600000) {
            neighbor.cell = measurement.cell;
            neighbor.area = measurement.area;
            neighbor.uuid = measurement.uuid;
            this.push(neighbor);
            break;
          }
        }
      }
      callback();
    }

    function parseGPS(data) {
      var gps = {};
      gps.localtime = parseLocalDateTime(data[0]);
      gps.lon = parsePosition(data[2]);
      gps.lat = parsePosition(data[3]);
      gps.altitude = parseFloat(data[4]);
      gps.timestamp = parseGPSDateTime(data[5] + data[6]);  
      gps.speed = parseFloat(data[8]);  
      gps.satellites = parseInt(data[9]);  
      gps.hdop = parseFloat(data[10]);  

      return gps;
    }

    function parseGSM(data, gps) {
      var measurement = {};
      measurement.source = 'celllogger';
      
      if (gps == null)
        return null;

      var localtime = parseLocalDateTime(data[0]);
      if (localtime > gps.localtime + 5000) {
        logger.log('Warning','Measurement with unknown GPS coordinates');
        return null;
      }

      if (data[3] == 'no measurement')
        return null;

      if (data[3] == 'JAMMING_STATE')
    	  return null;
      
      measurement.timestamp = gps.timestamp + localtime - gps.localtime;
      measurement.location = [gps.lon, gps.lat];
      measurement.altitude = gps.altitude;
      measurement.speed = gps.speed;
      measurement.satellites = gps.satellites;
      measurement.hdop = gps.hdop;

      measurement.radio = data[1];
      measurement.mcc = parseInt(data[2].slice(0,3));
      measurement.net = parseInt(data[2].slice(3));
      measurement.serving = (data[4] == 'SERVING');
      measurement.area = parseInt(data[5], 16);
      measurement.cell = parseInt(data[7], 16);

      measurement.uuid=measurement.mcc.toString() +'-'+ measurement.net.toString() +'-'+ (measurement.area || 0).toString()+'-'+measurement.cell.toString();
      measurement.channel = parseInt(data[8]);
      measurement.signal = parseFloat(data[9]);

      return validate(measurement, data);
    }

    function parseUMTS(data, gps, measurements, neighbors) {
      var measurement = {};
      measurement.source = 'celllogger';
      
      if (gps == null)
        return null;

      var localtime = parseLocalDateTime(data[0]);
      if (localtime > gps.localtime + 5000) {
        logger.log('Warning','Measurement with unknown GPS coordinates');
        return null;
      }

      if (data[3] == 'no measurement')
        return null;

      if (data[3] == 'JAMMING_STATE')
    	  return null;
    
      measurement.timestamp = gps.timestamp + localtime - gps.localtime;
      measurement.location = [gps.lon, gps.lat];
      measurement.altitude = gps.altitude;
      measurement.speed = gps.speed;
      measurement.satellites = gps.satellites;
      measurement.hdop = gps.hdop;

      measurement.radio = data[1];
      measurement.mcc = parseInt(data[2].slice(0,3));
      measurement.net = parseInt(data[2].slice(3));
      measurement.serving = (data[4] == 'SERVING');
      measurement.area = parseInt(data[5], 16);
      measurement.cell = parseInt(data[6], 16) * 65536 + parseInt(data[7], 16);
      measurement.uuid=measurement.mcc.toString() +'-'+ measurement.net.toString() +'-'+ (measurement.area || 0).toString()+'-'+measurement.cell.toString();
      measurement.channel = parseInt(data[8]);
      measurement.signal = parseFloat(data[9]);
      measurement.unit = parseInt(data[10]);

      if (measurement.serving) {
        measurements.push(measurement);
      	return validate(measurement, data);
      } else {
        neighbors.push(measurement); 
        return null;
      }
    }

    function parseLTE(data, gps,measurements,neighbors) {
      var measurement = {};
      measurement.source = 'celllogger';
      
      if (gps == null)
        return null;

      var localtime = parseLocalDateTime(data[0]);
      if (localtime > gps.localtime + 5000) {
        logger.log('Warning','Measurement with unknown GPS coordinates');
        return null;
      }

      if (data[3] == 'no measurement')
        return null;

      measurement.timestamp = gps.timestamp + localtime - gps.localtime;
      measurement.location = [gps.lon, gps.lat];
      measurement.altitude = gps.altitude;
      measurement.speed = gps.speed;
      measurement.satellites = gps.satellites;
      measurement.hdop = gps.hdop;

      measurement.radio = data[1];
      measurement.mcc = parseInt(data[2].slice(0,3));
      measurement.net = parseInt(data[2].slice(3));
      measurement.serving = (data[4] == 'SERVING');
      measurement.area = parseInt(data[5]);
      measurement.unit = parseInt(data[6], 16);
      measurement.cell = parseInt(data[7]);
      measurement.uuid=measurement.mcc.toString() +'-'+ measurement.net.toString() +'-'+ (measurement.area || 0).toString()+'-'+measurement.cell.toString();
      measurement.channel = parseInt(data[8]);
      measurement.signal = parseFloat(data[9]);

      if (measurement.serving) {
        measurements.push(measurement);
      	return validate(measurement, data);
      } else {
        neighbors.push(measurement); 
        return null;
      }
    }

    function parseWIFI(data,gps,measurements){
      var measurement = {};
      measurement.source = 'celllogger';
      if (gps == null)
        return null;

      var localtime = parseLocalDateTime(data[0]);
      if (localtime > gps.localtime + 5000) {
        logger.log('Warning','Measurement with unknown GPS coordinates');
        return null;
      }
    
      if (data[3] == 'JAMMING_STATE')
    	  return null;
      
      
      
      measurement.timestamp = gps.timestamp + localtime - gps.localtime;
      measurement.location = [gps.lon, gps.lat];
      measurement.altitude = gps.altitude;
      measurement.speed = gps.speed;
      measurement.satellites = gps.satellites;
      measurement.hdop = gps.hdop;

      measurement.bssid=data[2];
      measurement.channel=parseInt(data[3]);
      measurement.frequency=parseInt(data[4]);
      measurement.ssid=data[5];
      measurement.mode=data[6];
      measurement.encryption=Boolean(data[7]);
      measurement.signal=parseInt(data[8]);
      measurement.quality=data[9];
      if(measurement.encryption){
        if(data[10]!=='null')
          measurement.encryptiontype=data[10];
        if(data[11]!=='null')
          measurement.authenticationtype=data[11];
      } else {
    	  measurement.encryptiontype="null";
      }
      measurements.push(measurement);
      return validateWifi(measurement, data);
    }

    function parseLocalDateTime(str) {
      var date = new Date(2000+parseInt(str.slice(0,2)), parseInt(str.slice(2,4))-1, parseInt(str.slice(4,6)), parseInt(str.slice(6,8)), parseInt(str.slice(8,10)), parseInt(str.slice(10,12)), 0);
      return date.getTime();
    }

    function parseGPSDateTime(str) {
      var date = new Date(2000+parseInt(str.slice(4,6)), parseInt(str.slice(2,4))-1, parseInt(str.slice(0,2)), parseInt(str.slice(6,8)), parseInt(str.slice(8,10)), parseInt(str.slice(10,12)), 0);
      return date.getTime();
    }

    function parsePosition(str) {
      var degrees = parseInt(str.split(' ')[0]);
      var minutes = parseFloat(str.split(' ')[1].slice(0, -1)) / 60;
      var negative = (str.slice(-1) == 'W') || (str.slice(-1) == 'S'); 

      var location = degrees + minutes;
      if (negative)
        location *= -1;

      return location;
    } 


    return celllog;
  }
};

function validateWifi(measurement,data) {
  if (measurement.timestamp > 0 &&
      measurement.location[0] >= -180 &&
      measurement.location[0] <= 180 &&
      measurement.location[1] >= -90 &&
      measurement.location[1] <= 90 &&
      measurement.satellites >= 0 &&
      measurement.hdop  >= 0 &&
      measurement.bssid.length > 0 &&
      measurement.channel > 0 &&
      measurement.frequency > 0 &&
      measurement.signal > -200)
    return measurement
  else {
      logger.log('error','WIFI measurement not valid ' + JSON.stringify(measurement) + data);
    return null
  }
}

function validate(measurement, data) {
  if (measurement.timestamp > 0 &&
      measurement.location[0] >= -180 &&
      measurement.location[0] <= 180 &&
      measurement.location[1] >= -90 &&
      measurement.location[1] <= 90 &&
      measurement.satellites >= 0 &&
      measurement.hdop  >= 0 &&
      measurement.radio.length >= 3 &&
      measurement.mcc > 0 &&
      measurement.mcc < 1000 &&
      measurement.net > 0 &&
      measurement.net < 1000 &&
      measurement.area > 0 &&
      measurement.area < 65535 &&
      measurement.cell > 0 &&
      measurement.cell < 268435456 &&
      measurement.channel > 0 &&
      measurement.signal > -200)
    return measurement
  else {
      logger.log('error','measurement not valid ' + JSON.stringify(measurement) + data);
    return null
  }
}

