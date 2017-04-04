const util = require('util'),
    async = require('async'),
    moment = require('moment'),
    sql = require('mssql');

class Sql {
  constructor(cb, config) {
    this.types = sql;

    this.connection = new sql.Connection({
      user: config.user,
      password: config.password,
      server: config.server, // You can use 'localhost\\instance' to connect to named instance
      database: config.database,

      options: {
        requestTimeout: 1000 * 60 * 10,
        encrypt: true // Use this if you're on Windows Azure
      }
    }, (err) => {
      if (err) {
        return cb(err);
      }
      setImmediate(cb, null, this);
    });
  }

  __typeCast(value) {
    if (value instanceof String) {
      return "'" + value.replace(/'/g, "''") + "'";
    } else if (value === null) {
      return "null"
    } else if (typeof value == "boolean") {
      return value ? 1 : 0
    } else {
      return value;
    }
  }

  batch(queryArray, cb, transaction) {
    let request;
    if (transaction) {
      request = this.connection.request(transaction);
    } else {
      request = this.connection.request();
    }
    request.multiple = true;

    request.query(queryArray.join(";"), (err, recordsets) => {
      cb(err);
    });
  }

  merge(tableName, key, fields, cb, transaction) {
    let select = [];
    let source = [];
    let target = [];
    let update = [];
    let keyIsString = key instanceof String || typeof key == "string";
    let injection = false;
    Object.keys(fields).forEach((field) => {
      if (/(--| |\/\*|\*\/|')/.test(field) && !injection) {
        injection = true;
      }
      select.push(`${this.__typeCast(fields[field])} as [${field}]`);
      source.push(`[${field}]`);
      target.push(`s.[${field}]`);
      let skip;
      if (keyIsString) {
        skip = field == key;
      } else {
        skip = key.indexOf(field) != -1;
      }
      if (!skip) {
        update.push(`t.[${field}] = s.[${field}]`)
      }
    })
    if (injection) {
      return cb("SQL injection warning");
    }
    let where = [];
    if (keyIsString) {
      where.push(`t.[${key}] = s.[${key}]`);
    } else {
      where = key.map((item) => {
        return `t.[${item}] = s.[${item}]`;
      })
    }
    this.query(`
            MERGE [dbo].[${tableName}] AS t
            USING (select ${select.join(',')}) AS s
            ON (${where.join(" and ")})
            WHEN NOT MATCHED BY TARGET
                THEN INSERT(${source.join(',')}) VALUES (${target.join(',')})
            ${update.length ? `WHEN MATCHED 
                THEN UPDATE SET ${update.join(',')}` : ``};
        `, null, cb, transaction);
  }

  update(tableName, key, fields, cb, transaction) {
    let update = [];
    let keyIsString = key instanceof String || typeof key == "string";
    let injection = false;
    Object.keys(fields).forEach((field) => {
      if (/(--| |\/\*|\*\/|')/.test(field) && !injection) {
        injection = true;
      }
      let skip;
      if (keyIsString) {
        skip = field == key;
      } else {
        skip = key.indexOf(field) != -1;
      }
      if (!skip) {
        update.push(`[${field}] = ${this.__typeCast(fields[field])}`)
      }
    })
    let where = [];
    if (keyIsString) {
      where.push(`[${key}] = ${fields[key]}`);
    } else {
      where = key.map((item) => {
        return `[${item}] = ${fields[item]}`;
      })
    }
    if (injection) {
      return cb("SQL injection warning");
    }
    this.query(`
        UPDATE [dbo].[${tableName}] SET ${update.join(',')} WHERE ${where.join(" and ")}
    `, null, cb, transaction);
  }

  insert(tableName, key, fields, cb, transaction) {
    let source = [];
    let target = [];
    let injection = false;
    Object.keys(fields).forEach((field) => {
      if (/(--| |\/\*|\*\/|')/.test(field) && !injection) {
        injection = true;
      }
      source.push("[" + field + "]");
      target.push(this.__typeCast(fields[field]));
    });
    if (injection) {
      return cb("SQL injection warning");
    }
    this.query(`INSERT INTO [dbo].[${tableName}] (${source.join(',')}) ${key ? ' OUTPUT Inserted.' + key : ''} VALUES (${target.join(',')})`, null, (err, result) => {
      cb && cb(err, result);
    }, transaction);
  }

  query(query, params, cb, transaction) {
    if (params === null) {
      let request;
      if (transaction) {
        request = this.connection.request(transaction);
      } else {
        request = this.connection.request();
      }
      request.query(query, (err, recordsets, affected) => {
        if (err) {
          //console.error(moment().utc().format("YYYY-MM-DD HH:mm:ss"), query, params, err)
          return cb({ detail: err, query: query });
        }
        cb(null, { dataset: [recordsets], affected: affected })
      });
    } else {
      let ps;
      if (transaction) {
        ps = new sql.PreparedStatement(transaction);
      } else {
        ps = new sql.PreparedStatement(this.connection);
      }
      let applyParams = [];
      let adaptedParams = {};
      for (let param in params) {
        if (params[param][1] === undefined) {
          params[param][1] = sql.VarChar(255);
        }
        ps.input(param, params[param][1]);
        adaptedParams[param] = params[param][0];
      }
      applyParams.push(adaptedParams);
      ps.prepare(query, (err) => {
        if (err) {
          //console.error(moment().utc().format("YYYY-MM-DD HH:mm:ss"), query, params, err)
          return cb({ detail: err, query: query, params: params });
        }

        ps.multiple = true;
        applyParams.push((err, recordsets, affected) => {
          if (err) {
            //console.error(moment().utc().format("YYYY-MM-DD HH:mm:ss"), query, params, err)
            return cb({ detail: err, query: query, params: params });
          }

          ps.unprepare((err) => {
            if (err) {
              //console.error(moment().utc().format("YYYY-MM-DD HH:mm:ss"), query, params, err)
              cb({ detail: err, query: query, params: params });
            }
            cb(null, { dataset: recordsets, affected: affected })
          });
        });
        ps.execute.apply(ps, applyParams);
      });
    }
  }

  transaction(cb) {
    let transaction = new this.connection.Transaction();
    transaction.begin((err) => {
      cb(err, transaction);
    });
  }
}

module.exports = Sql;