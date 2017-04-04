const path = require('path');
const Sql = require(path.join(__dirname, '/helpers/sql.js'));
const fs = require('fs');
const colors = require('colors');
const async = require('async');
const moment = require('moment');

class Devices {
  constructor(core) {
    this.__core = core;
    this.loaded = 0;
    this.relations = {};

    this.__core.on('ready', () => {
      this.run();
    });

    new Sql((err, resp) => {
      this.sqlOld = resp;
      console.log('Connected to MAPIT 1 Database'.green);
      this.moduleLoaded();
    }, this.__core.config.mssql_old);

    new Sql((err, resp) => {
      this.sqlNew = resp;
      console.log('Connected to MAPIT 2 Database'.green);
      this.moduleLoaded();
    }, this.__core.config.mssql_new);

  }

  getDevice(Number, cb) {
    this.sqlNew.query('select * from [dbo].[Devices] d where d.[Number] = @number',
        { 'number': [Number, this.sqlNew.types.VarChar()] }, (err, rows) => {
          if (err) {
            return cb(err);
          }
          cb(null, rows.dataset[0][0]);
        });
  }

  run() {
    this.getDevices((err, resp) => {
      if (err) {
        return console.log({ err: err.red });
      }
      async.eachLimit(resp, 1, (item, callback) => {
        this.getDevice(item.Number, (e, r) => {
          if (!e && r) {
            console.log(`#${item.Number} exists`.magenta);
            return callback();
          }
          if (this.__core.config.ignore_devices.indexOf(item.Number) != -1) {
            console.log(`#${item.Number} was ignored by migration's rules`.yellow);
            return callback();
          }
          this.createDevice(item, (error, response) => {
            error && console.log(error.red);
            response && console.log(response.magenta);
            callback();
          });
        });
      }, (err) => {
        err && console.log({ err: err.red });
        this.saveRelations((err) => {
          if (err) {
            return console.log({ err });
          }
          console.log('Relations are saved'.green);
          this.disconnect();
        });
      });
    });
  }

  createDevice(item, cb) {
    this.sqlNew.insert('Devices', 'Id', {
      Number: new String(item.Number),
      Active: 1,
      Tenant: 1,
      Detail: new String(JSON.stringify(item)),
    }, (err, resp) => {
      if (err) {
        return cb(err);
      }
      cb(null, resp.dataset[0][0]);
    });
  }

  saveRelations(cb) {
    cb();
  }

  disconnect() {
    // close connections
    this.sqlOld.connection.close();
    console.log('Closed connection with MAPIT 1 Database'.green);
    this.sqlNew.connection.close();
    console.log('Closed connection with MAPIT 2 Database'.green);
  }

  getDevices(cb) {
    // get list of licenses' types
    this.sqlOld.query('select * from [dbo].[Devices]',
        null, (err, rows) => {
          if (err) {
            return cb(err);
          }
          cb(null, rows.dataset[0]);
        });
  }

  moduleLoaded() {
    this.loaded++;
    if (this.loaded === 2) {
      this.__core.emit('loaded', 'Devices module is loaded and ready to start');
    }
  }
}

module.exports = Devices;
