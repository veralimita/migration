const path = require('path');
const Sql = require(path.join(__dirname, '/helpers/sql.js'));
const fs = require('fs');
const colors = require('colors');
const async = require('async');
const moment = require('moment');

class Licenses {
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

  run() {
    this.getLicenses((err, resp) => {
      if (err) {
        return console.log({ err: err.red });
      }
      this.getRelations(() => {
        this.relations.rows || (this.relations.rows = {});
        async.eachLimit(resp, 1, (item, callback) => {
          this.processItem(item, callback);
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
    });
  }

  processItem(item, cb) {
    if (this.relations.rows[item['Id']] && this.relations.rows[item['Id']]['mapit1']) {
      console.log(`founded for #${item['Id']} MAPIT 1 object`.yellow);
    } else {
      console.log(`saved #${item['Id']} for MAPIT 1 object`.green);
      this.relations.rows[item['Id']] = { mapit1: item };
    }
    if (this.relations.rows[item['Id']] && this.relations.rows[item['Id']]['mapit2']) {
      console.log(`founded for #${item['Id']} MAPIT 2 object`.yellow);
      this.sqlNew.query('select * from [dbo].[Licenses] l where l.[Id] = @id',
          { 'id': [parseInt(this.relations.rows[item['Id']]['mapit2']['Id']), this.sqlNew.types.BigInt()] }, (err, rows) => {
            if (err || !rows.dataset[0][0]) {
              this.relations.rows[item['Id']]['mapit2'] = null;
              return this.processItem(item, cb);
            }
            cb();
            console.log(JSON.stringify(rows.dataset[0][0]).magenta);
          });

    } else {
      console.log(`creating #${item['Id']} for MAPIT 2 object`.green);
      this.sqlNew.insert('Licenses', 'Id', {
        Name: new String(item.Name),
        Description: new String(item.Description),
        Duration: item.Duration,
      }, (err, resp) => {
        if (err) {
          return cb(err);
        }
        this.relations.rows[item['Id']]['mapit2'] = resp.dataset[0][0];
        cb();
      });
    }
  }

  disconnect() {
    // close connections
    this.sqlOld.connection.close();
    console.log('Closed connection with MAPIT 1 Database'.green);
    this.sqlNew.connection.close();
    console.log('Closed connection with MAPIT 2 Database'.green);
  }

  saveRelations(cb) {
    this.relations.lastExec = moment().format(this.__core.config.moment_format);
    fs.writeFile(path.join(__dirname, '/relations/licenses.json'), JSON.stringify(this.relations), function (err) {
      cb(err);
    });
  }

  getRelations(cb) {
    fs.readFile(path.join(__dirname, '/relations/licenses.json'), 'utf8', (err, data) => {
      if (err) {
        console.log('file with relations doesn´t found'.yellow);
        return cb();
      }
      try {
        this.relations = JSON.parse(data);
      }
      catch (e) {
        console.log('damaged file with relations'.red);
      }
      return cb();
    });
  }

  getLicenses(cb) {
    // get list of licenses' types
    this.sqlOld.query('select * from [dbo].[Licences]',
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
      this.__core.emit('loaded', 'Licenses module is loaded and ready to start');
    }
  }
}

module.exports = Licenses;
