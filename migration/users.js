const path = require('path');
const Sql = require(path.join(__dirname, '/helpers/sql.js'));
const fs = require('fs');
const colors = require('colors');
const async = require('async');
const moment = require('moment');

class Users {
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
    async.eachLimit(this.__core.config.users, 1, (user, callback) => {
      this.processUser(user);
      this.getDevices(user, (err, resp) => {
        if (err) {
          console.log(err.red);
          return callback();
        }
        this.processDevices(resp, user, callback);
      });
    }, (err) => {
      console.log('ALL DATA PROCESSED'.green);
      this.disconnect();
    });
  }

  processUser(user) {
    this.getOldUser(user, (err, response) => {
      this.updateUser(user, response[0], (e, r) => {
        console.log({ e, r });
      });
    });
  };

  updateUser(user, userDetail, cb) {
    this.sqlNew.update('Supervisors', 'Id', {
      Id: user.newId,
      Detail: new String(JSON.stringify(userDetail))
    }, (err, result) => {
      cb(err, result);
    });
  }

  processDevices(list, user, cb) {
    async.eachLimit(list, 1, (device, callback) => {
      console.log(`#${device.Number} is processing`.green);
      this.getDevice(device.Number, (err, resp) => {
        if (err || !resp) {
          console.log((err || `#${device.Number} not found`).red);
          return callback();
        }
        async.waterfall([
          (callback) => {
            if (resp.ContainerId) {
              console.log(resp.Number.yellow, resp.ContainerId.yellow);
              return setImmediate(callback, null, resp.ContainerId);
            }
            this.createContainer(device.Name, user, (e, r) => {
              if (e) {
                console.log(e.red);
                return callback(e);
              }
              callback(null, r[0].Id);
            });
          },
          (containerId, callback) => {
            this.updateDevice(resp.Id, containerId, (err, resp) => {
              if (err) {
                console.log(err.red);
              }
              callback(null, containerId);
            });
          },
          (containerId, callback) => {
            this.getLinks(containerId, user.newId, (err, resp) => {
              if (err) {
                console.log(err.red);
              }
              callback(null, resp, containerId);
            });
          },
          (links, containerId, callback) => {
            if (links.length) {
              return setImmediate(callback, null, containerId);
            }
            this.createLinks(containerId, user.newId, (err, resp) => {
              callback(err, containerId)
            });
          }
        ], (err, containerId) => {
          if (err) {
            console.log({ err });
          } else {
            console.log(`#${containerId}  ---   DONE`.green);
          }
          callback();
        });
      });
    }, (err) => {
      if (err) {
        console.log(err.red);
      }
      cb();
    });
  }

  getLinks(containerId, userId, cb) {
    this.sqlNew.query('select * FROM [dbo].[Supervisor2Container] s Where s.[SupervisorId]=@userId and s.[ContainerId]=@containerId and s.[Archive] = 0',
        {
          'userId': [userId, this.sqlNew.types.BigInt()],
          'containerId': [containerId, this.sqlNew.types.BigInt()]
        }, (err, rows) => {
          if (err) {
            return cb(err);
          }
          cb(null, rows.dataset[0]);
        });
  }

  createLinks(containerId, userId, cb) {
    this.sqlNew.insert('Supervisor2Container', undefined, {
      ContainerId: containerId,
      SupervisorId: userId,
      Archive: 0
    }, (err) => {
      if (err) {
        return cb(err);
      }
      cb(null, containerId)
    })
  }

  updateDevice(deviceId, containerId, cb) {
    this.sqlNew.update('Devices', 'Id', {
      Id: deviceId,
      ContainerId: containerId
    }, (err, result) => {
      cb(err, result);
    });
  }

  createContainer(name, user, cb) {
    this.sqlNew.insert('Containers', 'Id', {
      Detail: new String(name),
      CreatorId: user.newId,
    }, (err, resp) => {
      if (err) {
        return cb(err);
      }
      cb(null, resp.dataset[0]);
    });
  }

  getDevices(user, cb) {
    this.sqlOld.query('select * from [dbo].[Devices] d where d.[UserId]=@user',
        { 'user': [user.oldId, this.sqlNew.types.BigInt()] }, (err, rows) => {
          if (err) {
            return cb(err);
          }
          cb(null, rows.dataset[0]);
        });
  }

  getOldUser(user, cb) {
    this.sqlOld.query('select * from [dbo].[AbpUsers] d where d.[Id]=@user',
        { 'user': [user.oldId, this.sqlOld.types.BigInt()] }, (err, rows) => {
          if (err) {
            return cb(err);
          }
          cb(null, rows.dataset[0]);
        });
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

  disconnect() {
    // close connections
    this.sqlOld.connection.close();
    console.log('Closed connection with MAPIT 1 Database'.green);
    this.sqlNew.connection.close();
    console.log('Closed connection with MAPIT 2 Database'.green);
  }

  moduleLoaded() {
    this.loaded++;
    if (this.loaded === 2) {
      this.__core.emit('loaded', 'Users module is loaded and ready to start');
    }
  }
}

module.exports = Users;
