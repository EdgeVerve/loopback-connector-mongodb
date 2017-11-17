/**
 * 
 * ©2016-2017 EdgeVerve Systems Limited (a fully owned Infosys subsidiary),
 * Bangalore, India. All Rights Reserved.
 * 
 */

/*!
 * Module dependencies
 */
var mongodb = require('mongodb');
var util = require('util');
var async = require('async');
var Connector = require('loopback-connector').Connector;
var debug = require('debug')('loopback:connector:mongodb');
var gFilterKeys = ["fieldReplace", "fieldValueReplace"];

/*!
 * Convert the id to be a BSON ObjectID if it is compatible
 * @param {*} id The id value
 * @returns {ObjectID}
 */
function ObjectID(id) {
  if (id instanceof mongodb.ObjectID) {
    return id;
  }
  if (typeof id !== 'string') {
    return id;
  }
  try {
    // MongoDB's ObjectID constructor accepts number, 12-byte string or 24-byte
    // hex string. For LoopBack, we only allow 24-byte hex string, but 12-byte
    // string such as 'line-by-line' should be kept as string
    if (/^[0-9a-fA-F]{24}$/.test(id)) {
      return new mongodb.ObjectID(id);
    } else {
      return id;
    }
  } catch (e) {
    return id;
  }
}

/*!
 * Generate the mongodb URL from the options
 */
function generateMongoDBURL(options) {
  options.hostname = (options.hostname || options.host || '127.0.0.1');
  options.port = (options.port || 27017);
  options.database = (options.database || options.db || 'test');
  var username = options.username || options.user;
  if (username && options.password) {
    return "mongodb://" + username + ":" + options.password + "@" + options.hostname + ":" + options.port + "/" + options.database;
  } else {
    return "mongodb://" + options.hostname + ":" + options.port + "/" + options.database;
  }
}

/**
 * Initialize the MongoDB connector for the given data source
 * @param {DataSource} dataSource The data source instance
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!mongodb) {
    return;
  }

  var s = dataSource.settings;

  s.safe = (s.safe !== false);
  s.w = s.w || 1;
  s.url = s.url || generateMongoDBURL(s);
  dataSource.connector = new MongoDB(s, dataSource);
  dataSource.ObjectID = mongodb.ObjectID;

  if (callback) {
    dataSource.connector.connect(callback);
  }
};

/**
 * The constructor for MongoDB connector
 * @param {Object} settings The settings object
 * @param {DataSource} dataSource The data source instance
 * @constructor
 */
function MongoDB(settings, dataSource) {
  Connector.call(this, 'mongodb', settings);

  this.debug = settings.debug || debug.enabled;

  if (this.debug) {
    debug('Settings: %j', settings);
  }

  this.dataSource = dataSource;
  if (this.settings.enableOptimisedfindOrCreate === true ||
    this.settings.enableOptimisedFindOrCreate === true ||
    this.settings.enableOptimizedfindOrCreate === true ||
    this.settings.enableOptimizedFindOrCreate === true) {
    MongoDB.prototype.findOrCreate = optimizedFindOrCreate;
  }

}

util.inherits(MongoDB, Connector);

/**
 * Connect to MongoDB
 * @param {Function} [callback] The callback function
 *
 * @callback callback
 * @param {Error} err The error object
 * @param {Db} db The mongo DB object
 */
MongoDB.prototype.connect = function (callback) {
  var self = this;
  if (self.db) {
    process.nextTick(function () {
      callback && callback(null, self.db);
    });
  } else {
    mongodb.MongoClient.connect(self.settings.url, self.settings, function (err, db) {
      if (!err) {
        if (self.debug) {
          debug('MongoDB connection is established: ' + self.settings.url);
        }
        self.db = db;
      } else {
        if (self.debug || !callback) {
          console.error('MongoDB connection is failed: ' + self.settings.url, err);
        }
      }
      callback && callback(err, db);
    });
  }
};

MongoDB.prototype.getTypes = function () {
  return ['db', 'nosql', 'mongodb'];
};

MongoDB.prototype.getDefaultIdType = function () {
  return ObjectID;
};

/**
 * Get collection name for a given model
 * @param {String} model Model name
 * @returns {String} collection name
 */
MongoDB.prototype.collectionName = function (model) {
  var modelClass = this._models[model];
  if (modelClass.settings.mongodb) {
    model = modelClass.settings.mongodb.collection || model;
  }
  return model;
};

/**
 * Access a MongoDB collection by model name
 * @param {String} model The model name
 * @returns {*}
 */
MongoDB.prototype.collection = function (model) {
  if (!this.db) {
    throw new Error('MongoDB connection is not established');
  }
  var collectionName = this.collectionName(model);
  return this.db.collection(collectionName);
};

/*!
 * Convert the data from database to JSON
 *
 * @param {String} model The model name
 * @param {Object} data The data from DB
 */
MongoDB.prototype.fromDatabase = function (model, data) {
  if (!data) {
    return null;
  }
  var props = this._models[model].properties;
  for (var p in props) {
    var prop = props[p];
    if (prop && prop.type === Buffer) {
      if (data[p] instanceof mongodb.Binary) {
        // Convert the Binary into Buffer
        data[p] = data[p].read(0, data[p].length());
      }
    } else if (prop && prop.type === String) {
      if (data[p] instanceof mongodb.Binary) {
        // Convert the Binary into String
        data[p] = data[p].toString();
      }
    }
  }
  return data;
};

/**
 * Execute a mongodb command
 * @param {String} model The model name
 * @param {String} command The command name
 * @param [...] params Parameters for the given command
 */
MongoDB.prototype.execute = function (model, command) {
  var collection = this.collection(model);
  // Get the parameters for the given command
  var args = [].slice.call(arguments, 2);
  // The last argument must be a callback function
  var callback = args[args.length - 1];
  var context = {
    model: model,
    collection: collection, req: {
      command: command,
      params: args
    }
  };
  this.notifyObserversAround('execute', context, function (context, done) {
    args[args.length - 1] = function (err, result) {
      if (err) {
        debug('Error: ', err);
      } else {
        context.res = result;
        debug('Result: ', result);
      }
      done(err, result);
    }
    debug('MongoDB: model=%s command=%s', model, command, args);
    return collection[command].apply(collection, args);
  }, callback);
};

MongoDB.prototype.coerceId = function (model, id) {
  // See https://github.com/strongloop/loopback-connector-mongodb/issues/206
  if (id == null) return id;
  var self = this;
  var idValue = id;
  var idName = self.idName(model);

  // Type conversion for id
  var idProp = self.getPropertyDefinition(model, idName);
  if (idProp && typeof idProp.type === 'function') {
    if (!(idValue instanceof idProp.type)) {
      idValue = idProp.type(id);
      if (idProp.type === Number && isNaN(id)) {
        // Reset to id
        idValue = id;
      }
    }
  }
  return idValue;
};

/**
 * Create a new model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [callback] The callback function
 */
MongoDB.prototype.create = function (model, data, options, callback) {
  var self = this;
  if (self.debug) {
    debug('create', model, data);
  }
  var idValue = self.getIdValue(model, data);
  var idName = self.idName(model);

  if (idValue === null) {
    delete data[idName]; // Allow MongoDB to generate the id
  } else {
    var oid = self.coerceId(model, idValue); // Is it an Object ID?c
    data._id = oid; // Set it to _id
    idName !== '_id' && delete data[idName];
  }
  this.execute(model, 'insert', data, { safe: true }, function (err, result) {
    if (self.debug) {
      debug('create.callback', model, err, result);
    }
    if (err) {
      return callback(err);
    }
    idValue = result.ops[0]._id;
    idValue = self.coerceId(model, idValue);
    // Wrap it to process.nextTick as delete data._id seems to be interferring
    // with mongo insert
    process.nextTick(function () {
      // Restore the data object
      delete data._id;
      data[idName] = idValue;
      callback(err, err ? null : idValue);
    });
  });
};

/**
 * Save the model instance for the given data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [callback] The callback function
 */
MongoDB.prototype.save = function (model, data, options, callback) {
  var self = this;
  if (self.debug) {
    debug('save', model, data);
  }
  var idValue = self.getIdValue(model, data);
  var idName = self.idName(model);

  var oid = self.coerceId(model, idValue);
  data._id = oid;
  idName !== '_id' && delete data[idName];

  this.execute(model, 'save', data, { w: 1 }, function (err, result) {
    if (!err) {
      self.setIdValue(model, data, idValue);
      idName !== '_id' && delete data._id;
    }
    if (self.debug) {
      debug('save.callback', model, err, result);
    }

    var info = {};
    if (result && result.result) {
      // create result formats:
      //   { ok: 1, n: 1, upserted: [ [Object] ] }
      //   { ok: 1, nModified: 0, n: 1, upserted: [ [Object] ] }
      //
      // update result formats:
      //   { ok: 1, n: 1 }
      //   { ok: 1, nModified: 1, n: 1 }
      if (result.result.ok === 1 && result.result.n === 1) {
        info.isNewInstance = !!result.result.upserted;
      } else {
        debug('save result format not recognized: %j', result.result);
      }
    }

    callback && callback(err, result && result.ops, info);
  });
};

/**
 * Check if a model instance exists by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param {Function} [callback] The callback function
 *
 */
MongoDB.prototype.exists = function (model, id, options, callback) {
  var self = this;
  if (self.debug) {
    debug('exists', model, id);
  }
  id = self.coerceId(model, id);
  this.execute(model, 'findOne', { _id: id }, function (err, data) {
    if (self.debug) {
      debug('exists.callback', model, id, err, data);
    }
    callback(err, !!(!err && data));
  });
};

/**
 * Find a model instance by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param {Function} [callback] The callback function
 */
MongoDB.prototype.find = function find(model, id, options, callback) {
  var self = this;
  if (self.debug) {
    debug('find', model, id);
  }
  var idName = self.idName(model);
  var oid = self.coerceId(model, id);
  this.execute(model, 'findOne', { _id: oid }, function (err, data) {
    if (self.debug) {
      debug('find.callback', model, id, err, data);
    }

    data = self.fromDatabase(model, data);
    data && idName !== '_id' && delete data._id;
    callback && callback(err, data);
  });
};

/**
 * Parses the data input for update operations and returns the
 * sanitised version of the object.
 *
 * @param data
 * @returns {*}
 */
MongoDB.prototype.parseUpdateData = function (model, data, options) {
  options = options || {};
  var parsedData = {};

  var modelClass = this._models[model];

  var allowExtendedOperators = this.settings.allowExtendedOperators;
  if (options.hasOwnProperty('allowExtendedOperators')) {
    allowExtendedOperators = options.allowExtendedOperators === true;
  } else if (allowExtendedOperators !== false && modelClass.settings.mongodb
    && modelClass.settings.mongodb.hasOwnProperty('allowExtendedOperators')) {
    allowExtendedOperators = modelClass.settings.mongodb.allowExtendedOperators === true;
  } else if (allowExtendedOperators === true) {
    allowExtendedOperators = true;
  }

  if (allowExtendedOperators) {
    // Check for other operators and sanitize the data obj
    var acceptedOperators = [
      // Field operators
      '$currentDate', '$inc', '$max', '$min', '$mul', '$rename', '$setOnInsert', '$set', '$unset',
      // Array operators
      '$addToSet', '$pop', '$pullAll', '$pull', '$pushAll', '$push',
      // Bitwise operator
      '$bit'
    ];

    var usedOperators = 0;

    // each accepted operator will take its place on parsedData if defined
    for (var i = 0; i < acceptedOperators.length; i++) {
      if (data[acceptedOperators[i]]) {
        parsedData[acceptedOperators[i]] = data[acceptedOperators[i]];
        usedOperators++;
      }
    }

    // if parsedData is still empty, then we fallback to $set operator
    if (usedOperators === 0) {
      parsedData.$set = data;
    }
  } else {
    parsedData.$set = data;
  }

  return parsedData;
};

/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Function} [callback] The callback function
 */
MongoDB.prototype.updateOrCreate = function updateOrCreate(model, data, options, callback) {
  var self = this;
  if (self.debug) {
    debug('updateOrCreate', model, data);
  }

  var id = self.getIdValue(model, data);
  var idName = self.idName(model);
  var oid = self.coerceId(model, id);
  delete data[idName];

  // Check for other operators and sanitize the data obj
  data = self.parseUpdateData(model, data, options);
  this.execute(model, 'findAndModify', {
    _id: oid
  }, [
      ['_id', 'asc']
    ], data, { upsert: true, new: true }, function (err, result) {
      if (self.debug) {
        debug('updateOrCreate.callback', model, id, err, result);
      }
      var object = result && result.value;
      if (!err && !object) {
        // No result
        err = 'No ' + model + ' found for id ' + id;
      }
      if (!err) {
        self.setIdValue(model, object, oid);
        object && idName !== '_id' && delete object._id;
      }

      var info;
      if (result && result.lastErrorObject) {
        info = { isNewInstance: !result.lastErrorObject.updatedExisting };
      } else {
        debug('updateOrCreate result format not recognized: %j', result);
      }

      callback && callback(err, object, info);
    });

};

/**
 * Delete a model instance by id
 * @param {String} model The model name
 * @param {*} id The id value
 * @param [callback] The callback function
 */
MongoDB.prototype.destroy = function destroy(model, id, options, callback) {
  var self = this;
  if (self.debug) {
    debug('delete', model, id);
  }
  id = self.coerceId(model, id);

  this.execute(model, 'remove', { _id: id }, function (err, result) {
    if (self.debug) {
      debug('delete.callback', model, id, err, result);
    }
    var res = result && result.result;
    if (res) {
      res.count = res.n;
    }
    callback && callback(err, res);
  });
};

/*!
 * Decide if id should be included
 * @param {Object} fields
 * @returns {Boolean}
 * @private
 */
function idIncluded(fields, idName) {
  if (!fields) {
    return true;
  }
  if (Array.isArray(fields)) {
    return fields.indexOf(idName) >= 0;
  }
  if (fields[idName]) {
    // Included
    return true;
  }
  if ((idName in fields) && !fields[idName]) {
    // Excluded
    return false;
  }
  for (var f in fields) {
    return !fields[f]; // If the fields has exclusion
  }
  return true;
}

/**
 * Build the aggregation pipeline based on filter
 * @param {Object} filter The model name
 * @returns {Array} pipeLine The aggregated pipeline array
 */
MongoDB.prototype.buildAggregationPipeLine = function buildAggregationPipeLine(filter) {
  debug('in building Aggregation PipeLine');
  var pipeLine = [];
  var fieldValueReplace = {};
  var condition = { '$cond': [{ '$eq': [] }] };
  // Checking for fieldValueReplace before fieldReplace since the value replacement 
  // should come prior to key replacement
  if (filter.fieldValueReplace) {
    for (var key in filter.fieldValueReplace) {
      if (filter.fieldValueReplace.hasOwnProperty(key)) {
        for (var inkey in filter.fieldValueReplace[key]) {
          if (typeof filter.fieldValueReplace[key] === 'object' && filter.fieldValueReplace[key].hasOwnProperty(inkey)) {
            // For a given collection of records. [{fn:'p',ln:'t',mn:'k'},{fn:'a',ln:'v',mn:'v'},{fn:'s',ln:'a',mn:'r'}]
            // and given 
            // { "fieldValueReplace": { "fn": { "a": "ajith", "p": "pradeep" }, "mn": { "r": "regis" } } }
            //
            // the below code creates a aggregation pipeline like below
            //  [ 
            //    { $project :{ "fn": {$cond: [{$eq: ["$fn", "a"]}, "ajith", "$fn"]}, "mn":1, "ln":1 } }, 
            //    { $project :{ "fn": {$cond: [{$eq: ["$fn", "p"]}, "pradeep", "$fn"]}, "mn":1, "ln":1 } },
            //    { $project :{ "mn": {$cond: [{$eq: ["$mn", "r"]}, "regis", "$mn"]}, "fn":1, "ln":1 } } 
            //  ]
            fieldValueReplace['$project'] = fieldValueReplace['$project'] || {};
            condition['$cond'][0]['$eq'].push("$" + key);
            condition['$cond'][0]['$eq'].push(inkey); // TODO boolean value comparison. Ex: fieldValueReplace:{"status":{"true":1,"false":2}}
            condition['$cond'].push(filter.fieldValueReplace[key][inkey]);
            condition['$cond'].push("$" + key);
            fieldValueReplace['$project'][key] = condition;
            projectFieldList(fieldValueReplace['$project'], filter.fieldList, key);
            pipeLine.push(fieldValueReplace);
            condition = { '$cond': [{ '$eq': [] }] };
            fieldValueReplace = {};
          }

        }
      }
    }

  }
  var excludeFieldList = [];
  if (filter.fieldReplace) {
    var individualArrays = [];
    // Sorting the fieldReplace keys
    var sortedArray = Object.keys(filter.fieldReplace).sort();
    // Constructing individual arrays of related fieldReplace keys
    // Ex: ['empid', 'org', 'cont', 'cont.addr', 'role', 'role.id']
    // -> [['empid'],['org'], ['cont', 'cont.addr'], ['role', 'role.id']]
    while (sortedArray.length != 0) {
      var tempArray = [];
      tempArray.push(sortedArray[0]);
      for (var i = 1; i < sortedArray.length; i++) {
        if (tempArray[0].split('.').shift() === sortedArray[i].split('.').shift()) {
          tempArray.push(sortedArray[i]);
          sortedArray.splice(i, 1);
          i--;
        }
      }
      sortedArray.splice(0, 1);
      individualArrays.push(tempArray);
    }
    // Sorting the individual arrays based on number of dot(.)s in descending order
    // [['cont.addr', 'cont'], ['role.id','role'], ['empid'], ['org']]
    for (var k = 0; k < individualArrays.length; k++) {
      if (individualArrays[k].length > 1) {
        var temp;
        for (var i = 1; i < individualArrays[k].length; i++) {
          for (var j = i; j > 0; j--) {
            if ((individualArrays[k][j].match(/\./g) || []).length > (individualArrays[k][j - 1].match(/\./g) || []).length) {
              temp = individualArrays[k][j];
              individualArrays[k][j] = individualArrays[k][j - 1];
              individualArrays[k][j - 1] = temp;
            }
          }
        }
      }
    }
    var projectArraylist = [];
    // Looping through while all the arrays are not empty.
    while (!containsAllEmptyArrays(individualArrays)) {
      var fieldReplace = {};
      fieldReplace['$project'] = {};
      for (var i = 0; i < individualArrays.length; i++) {
        if (individualArrays[i][0]) projectArraylist.push(individualArrays[i][0]);
        for (var j = 0; j < individualArrays[i].length; j++) {
          if (individualArrays[i][j + 1] && (individualArrays[i][0].match(/\./g) || []).length == (individualArrays[i][j + 1].match(/\./g) || []).length) {
            projectArraylist.push(individualArrays[i][j + 1]);
            individualArrays[i].splice(j + 1, 1);
            j--;
          }
        }
        individualArrays[i].splice(0, 1);
      }
      for (var k = 0; k < projectArraylist.length; k++) {
        var lastIndexOfPeriod = projectArraylist[k].lastIndexOf('.');
        if (lastIndexOfPeriod !== -1) {
          // For supported nested keys Ex: { "fieldReplace" : {"customer.name": "customer_name"} }
          // projecting as { $project: {"customer.customer_name": "$customer.name"},  "emplId": 1}
          fieldReplace['$project'][projectArraylist[k].substring(0, lastIndexOfPeriod) + "." + filter.fieldReplace[projectArraylist[k]]] = "$" + projectArraylist[k];
        } else {
          fieldReplace['$project'][filter.fieldReplace[projectArraylist[k]]] = "$" + projectArraylist[k];
        }
      }
      for (var m = 0; m < projectArraylist.length; m++) {
        for (var n = 0; n < filter.fieldList.length; n++) {
          if (filter.fieldList[n].indexOf(projectArraylist[m] + '.') !== -1) {
            filter.fieldList.splice(n, 1);
            n--;
          }
        }
      }
      projectFieldList(fieldReplace['$project'], filter.fieldList, projectArraylist);
      projectArraylist = [];
      filter.fieldList = Object.keys(fieldReplace['$project']);
      pipeLine.push(fieldReplace);
    }
  }
  function projectFieldList(project, fieldList, excludeList) {
    if (fieldList && Array.isArray(fieldList)) {
      for (var i = 0; i < fieldList.length; i++) {
        if (excludeList.indexOf(fieldList[i]) == -1)
          project[fieldList[i]] = 1;
      }
    }
  }
  function containsAllEmptyArrays(arrays) {
    for (var i = 0; i < arrays.length; i++) {
      if (arrays[i].length !== 0)
        return false;
    }
    return true;
  }
  return pipeLine;
};

MongoDB.prototype.buildWhere = function (model, where) {
  var self = this;
  var query = {};
  if (where === null || (typeof where !== 'object')) {
    return query;
  }
  var idName = self.idName(model);
  Object.keys(where).forEach(function (k) {
    var cond = where[k];
    if (k === 'and' || k === 'or' || k === 'nor') {
      if (Array.isArray(cond)) {
        cond = cond.map(function (c) {
          return self.buildWhere(model, c);
        });
      }
      query['$' + k] = cond;
      delete query[k];
      return;
    }
    if (k === idName) {
      k = '_id';
    }
    var propName = k;
    if (k === '_id') {
      propName = idName;
    }
    var prop = self.getPropertyDefinition(model, propName);

    var spec = false;
    var options = null;
    if (cond && cond.constructor.name === 'Object') {
      options = cond.options;
      spec = Object.keys(cond)[0];
      cond = cond[spec];
    }
    if (spec) {
      if (spec === 'between') {
        query[k] = { $gte: cond[0], $lte: cond[1] };
      } else if (spec === 'inq') {
        query[k] = {
          $in: cond.map(function (x) {
            if ('string' !== typeof x || prop.type !== ObjectID) return x;
            return ObjectID(x);
          })
        };
      } else if (spec === 'nin') {
        query[k] = {
          $nin: cond.map(function (x) {
            if ('string' !== typeof x || prop.type !== ObjectID) return x;
            return ObjectID(x);
          })
        };
      } else if (spec === 'like') {
        query[k] = { $regex: new RegExp(cond, options) };
      } else if (spec === 'nlike') {
        query[k] = { $not: new RegExp(cond, options) };
      } else if (spec === 'neq') {
        query[k] = { $ne: cond };
      } else if (spec === 'regexp') {
        if (cond.global)
          console.warn('MongoDB regex syntax does not respect the `g` flag');

        query[k] = { $regex: cond };
      }
      else {
        query[k] = {};
        query[k]['$' + spec] = cond;
      }
    } else {
      if (cond === null) {
        // http://docs.mongodb.org/manual/reference/operator/query/type/
        // Null: 10
        query[k] = { $type: 10 };
      } else {
        if (k === '_id') {
          query[k] = ObjectID(cond);
        }
        else {
          query[k] = cond;
        }
      }
    }
  });
  return query;
};

MongoDB.prototype.buildSort = function (model, order) {
  var sort = {}, idName = this.idName(model);
  if (!order) {
    var idNames = this.idNames(model);
    if (idNames && idNames.length) {
      order = idNames;
    }
  }
  if (order) {
    var keys = order;
    if (typeof keys === 'string') {
      keys = keys.split(',');
    }
    for (var index = 0, len = keys.length; index < len; index++) {
      var m = keys[index].match(/\s+(A|DE)SC$/);
      var key = keys[index];
      key = key.replace(/\s+(A|DE)SC$/, '').trim();
      if (key === idName) {
        key = '_id';
      }
      if (m && m[1] === 'DE') {
        sort[key] = -1;
      } else {
        sort[key] = 1;
      }
    }
  } else {
    // order by _id by default
    sort = { _id: 1 };
  }
  return sort;
};

/**
 * Find matching model instances by the filter including gFilterKeys
 *
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Function} [callback] The callback function
 */
MongoDB.prototype.evall = function evall(model, filter, options, callback) {
  var self = this;
  if (self.debug) {
    debug('evall', model, filter);
  }
  var pipeLine = self.buildAggregationPipeLine(filter);
  debug('aggregation pipeLine to be executed ', JSON.stringify(pipeLine, null, 2));
  this.execute(model, 'aggregate', pipeLine, processResponse);
  var idName = self.idName(model);
  var fields = filter.fields;
  function processResponse(err, data) {
    // currently the second parameter 'data' is coming as an array but not as a cursor.
    // TODO Get the cursor and implement other features. Ex: Sort, Limit etc
    if (err) {
      return callback(err);
    }
    var objs = data.map(function (o) {
      if (idIncluded(fields, self.idName(model))) {
        self.setIdValue(model, o, o._id);
      }
      // Don't pass back _id if the fields is set
      if (fields || idName !== '_id') {
        delete o._id;
      }
      o = self.fromDatabase(model, o);
      return o;
    });
    if (filter && filter.include) {
      self._models[model].model.include(objs, filter.include, options, callback);
    } else {
      callback(null, objs);
    }
  }
};


/**
 * Find matching model instances by the filter
 *
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Function} [callback] The callback function
 */
MongoDB.prototype.all = function all(model, filter, options, callback) {
  var self = this;
  for (var i = 0; i < gFilterKeys.length; i++) {
    if (filter[gFilterKeys[i]] !== undefined) {
      debug('gFilterKeys matched, calling evall and returning');
      self.evall(model, filter, options, callback);
      return;
    }
  }
  if (self.debug) {
    debug('all', model, filter);
  }
  filter = filter || {};
  var idName = self.idName(model);
  var query = {};
  if (filter.where) {
    if (filter.where[idName]) {
      var id = filter.where[idName];
      delete filter.where[idName];
      if (id.constructor !== Object) {
        // {id: value}
        id = self.coerceId(model, id);
      }
      filter.where._id = id;
    }
    query = self.buildWhere(model, filter.where);
  }
  if (filter && filter.group) {
    this.modifyFilter(filter);
    var pipeline = this.buildPipeline(model, filter, query);
  }
  var fields = filter.fields;
  var groups = filter.group;
  if (groups) {
    // this.executeAggregation(model, 'aggregate', pipeline, { cursor: {} }, processAggregationResponse);
    this.execute(model, 'aggregate', pipeline, {}, processAggregationResponse);
  } else if (fields) {
    this.execute(model, 'find', query, fields, processResponse);
  } else {
    this.execute(model, 'find', query, processResponse);
  }

  function processResponse(err, cursor) {
    if (err) {
      return callback(err);
    }
    var order = {};
    if (!filter.order) {
      var idNames = self.idNames(model);
      if (idNames && idNames.length) {
        filter.order = idNames;
      }
    }
    if (filter.order) {
      var keys = filter.order;
      if (typeof keys === 'string') {
        keys = keys.split(',');
      }
      for (var index = 0, len = keys.length; index < len; index++) {
        var m = keys[index].match(/\s+(A|DE)SC$/);
        var key = keys[index];
        key = key.replace(/\s+(A|DE)SC$/, '').trim();
        if (key === idName) {
          key = '_id';
        }
        if (m && m[1] === 'DE') {
          order[key] = -1;
        } else {
          order[key] = 1;
        }
      }
    } else {
      // order by _id by default
      order = { _id: 1 };
    }
    cursor.sort(order);

    if (filter.limit) {
      cursor.limit(filter.limit);
    }
    if (filter.skip) {
      cursor.skip(filter.skip);
    } else if (filter.offset) {
      cursor.skip(filter.offset);
    }
    cursor.toArray(function (err, data) {
      if (self.debug) {
        debug('all', model, filter, err, data);
      }
      if (err) {
        return callback(err);
      }
      var objs = data.map(function (o) {
        if (idIncluded(fields, self.idName(model))) {
          self.setIdValue(model, o, o._id);
        }
        // Don't pass back _id if the fields is set
        if (fields || idName !== '_id') {
          delete o._id;
        }
        o = self.fromDatabase(model, o);
        return o;
      });
      if (filter && filter.include) {
        self._models[model].model.include(objs, filter.include, options, callback);
      } else {
        callback(null, objs);
      }
    });
  }

  function processAggregationResponseWithCursor(cursor) {
    var order = {};
    if (filter.order) {
      var keys = filter.order;
      if (typeof keys === 'string') {
        keys = keys.split(',');
      }
      for (var index = 0, len = keys.length; index < len; index++) {
        var m = keys[index].match(/\s+(A|DE)SC$/);
        var key = keys[index];
        key = key.replace(/\s+(A|DE)SC$/, '').trim();
        if (key === idName) {
          key = '_id';
        }
        if (m && m[1] === 'DE') {
          order[key] = -1;
        } else {
          order[key] = 1;
        }
      }
    }
    if (filter.group) {
      Object.keys(filter.group).forEach(function (key) {
        key = key.toLowerCase();
        if (key !== 'groupby') {
          var val = filter.group[key];
          Object.keys(val).forEach(function (elem) {
            filter.fields.push(val[elem]);
          });
        }
      });
    }
    cursor.sort(order);
    if (filter.limit) {
      cursor.limit(filter.limit);
    }
    if (filter.skip) {
      cursor.skip(filter.skip);
    } else if (filter.offset) {
      cursor.skip(filter.offset);
    }
    cursor.toArray(function (err, data) {
      if (self.debug) {
        debug('all', model, filter, err, data);
      }
      if (err) {
        return callback(err);
      }
      var objs = data.map(function (o) {
        o = self.fromDatabase(model, o);
        return o;
      });
      callback(null, objs);
    });
  }

  function processAggregationResponse(err, data) {
    if (err) {
      return callback(err);
    }
    var objs = data;
    if (filter.group && filter.group.groupBy) {
      Object.keys(filter.group).forEach(function (key) {
        key = key.toLowerCase();
        if (key !== 'groupby') {
          var val = filter.group[key];
          Object.keys(val).forEach(function (elem) {
            filter.fields.push(val[elem]);
          });
        }
      });
      objs = data.map(function (obj) {
        return self.fromRow(model, obj);
      });
    }
    callback(null, objs);
  }
};

/**
 * Delete all instances for the given model
 * @param {String} model The model name
 * @param {Object} [where] The filter for where
 * @param {Function} [callback] The callback function
 */
MongoDB.prototype.destroyAll = function destroyAll(model, where, options, callback) {
  var self = this;
  if (self.debug) {
    debug('destroyAll', model, where);
  }
  if (!callback && 'function' === typeof where) {
    callback = where;
    where = undefined;
  }

  where = self.buildWhere(model, where);
  this.execute(model, 'remove', where || {}, function (err, info) {
    if (err) return callback && callback(err);

    if (self.debug)
      debug('destroyAll.callback', model, where, err, info);

    var affectedCount = info.result ? info.result.n : undefined;

    callback && callback(err, { count: affectedCount });
  });
};

/**
 * Count the number of instances for the given model
 *
 * @param {String} model The model name
 * @param {Function} [callback] The callback function
 * @param {Object} filter The filter for where
 *
 */
MongoDB.prototype.count = function count(model, where, options, callback) {
  var self = this;
  if (self.debug) {
    debug('count', model, where);
  }
  where = self.buildWhere(model, where);
  this.execute(model, 'count', where, function (err, count) {
    if (self.debug) {
      debug('count.callback', model, err, count);
    }
    callback && callback(err, count);
  });
};

/**
 * Update properties for the model instance data
 * @param {String} model The model name
 * @param {Object} data The model data
 * @param {Function} [callback] The callback function
 */
MongoDB.prototype.updateAttributes = function updateAttrs(model, id, data, options, cb) {
  var self = this;

  // Check for other operators and sanitize the data obj
  data = self.parseUpdateData(model, data, options);

  if (self.debug) {
    debug('updateAttributes', model, id, data);
  }
  var oid = self.coerceId(model, id);
  var idName = this.idName(model);

  this.execute(model, 'findAndModify', { _id: oid }, [
    ['_id', 'asc']
  ], data, {}, function (err, result) {
    if (self.debug) {
      debug('updateAttributes.callback', model, id, err, result);
    }
    var object = result && result.value;
    if (!err && !object) {
      // No result
      err = 'No ' + model + ' found for id ' + id;
    }
    self.setIdValue(model, object, id);
    object && idName !== '_id' && delete object._id;
    cb && cb(err, object);
  });
};

/**
 * Update all matching instances
 * @param {String} model The model name
 * @param {Object} where The search criteria
 * @param {Object} data The property/value pairs to be updated
 * @callback {Function} cb Callback function
 */
MongoDB.prototype.update =
  MongoDB.prototype.updateAll = function updateAll(model, where, data, options, cb) {
    var self = this;
    if (self.debug) {
      debug('updateAll', model, where, data);
    }
    var idName = this.idName(model);

    where = self.buildWhere(model, where);
    delete data[idName];

    // Check for other operators and sanitize the data obj
    data = self.parseUpdateData(model, data, options);

    this.execute(model, 'update', where, data, { multi: true, upsert: false },
      function (err, info) {
        if (err) return cb && cb(err);

        if (self.debug)
          debug('updateAll.callback', model, where, data, err, info);

        var affectedCount = info.result ? info.result.n : undefined;

        cb && cb(err, { count: affectedCount });
      });
  };

/**
 * Disconnect from MongoDB
 */
MongoDB.prototype.disconnect = function (cb) {
  if (this.debug) {
    debug('disconnect');
  }
  if (this.db) {
    this.db.close();
  }
  if (cb) {
    process.nextTick(cb);
  }
};

/**
 * Perform autoupdate for the given models. It basically calls createIndex
 * @param {String[]} [models] A model name or an array of model names. If not
 * present, apply to all models
 * @param {Function} [cb] The callback function
 */
MongoDB.prototype.autoupdate = function (models, cb) {
  var self = this;
  if (self.db) {
    if (self.debug) {
      debug('autoupdate');
    }
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(self._models);

    async.each(models, function (model, modelCallback) {
      var indexes = self._models[model].settings.indexes || [];
      var indexList = [];
      var index = {};
      var options = {};

      if (typeof indexes === 'object') {
        for (var indexName in indexes) {
          index = indexes[indexName];
          if (index.keys) {
            // The index object has keys
            options = index.options || {};
            options.name = options.name || indexName;
            index.options = options;
          } else {
            options = { name: indexName };
            index = {
              keys: index,
              options: options
            };
          }
          indexList.push(index);
        }
      } else if (Array.isArray(indexes)) {
        indexList = indexList.concat(indexes);
      }

      var properties = self._models[model].properties;
      for (var p in properties) {
        if (properties[p].index) {
          index = {};
          index[p] = 1; // Add the index key
          if (typeof properties[p].index === 'object') {
            // If there is a mongodb key for the index, use it
            if (typeof properties[p].index.mongodb === 'object') {
              options = properties[p].index.mongodb;
              index[p] = options.kind || 1;

              // Backwards compatibility for former type of indexes
              if (properties[p].index.unique === true) {
                options.unique = true;
              }

            } else {
              // If there isn't an  properties[p].index.mongodb object, we read the properties from  properties[p].index
              options = properties[p].index;
            }

            if (options.background === undefined) {
              options.background = true;
            }
            // If properties[p].index isn't an object we hardcode the background option and check for properties[p].unique
          } else {
            options = { background: true };
            if (properties[p].unique) {
              options.unique = true;
            }
          }
          indexList.push({ keys: index, options: options });
        }
      }

      if (self.debug) {
        debug('create indexes: ', indexList);
      }

      async.each(indexList, function (index, indexCallback) {
        if (self.debug) {
          debug('createIndex: ', index);
        }
        self.collection(model).createIndex(index.fields || index.keys, index.options, indexCallback);
      }, modelCallback);

    }, cb);
  } else {
    self.dataSource.once('connected', function () {
      self.autoupdate(models, cb);
    });
  }
};

/**
 * Perform automigrate for the given models. It drops the corresponding collections
 * and calls createIndex
 * @param {String[]} [models] A model name or an array of model names. If not present, apply to all models
 * @param {Function} [cb] The callback function
 */
MongoDB.prototype.automigrate = function (models, cb) {
  var self = this;
  if (self.db) {
    if (self.debug) {
      debug('automigrate');
    }
    if ((!cb) && ('function' === typeof models)) {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(self._models);

    // Make it serial as multiple models might map to the same collection
    async.eachSeries(models, function (model, modelCallback) {

      var collectionName = self.collectionName(model);
      if (self.debug) {
        debug('drop collection %s for model %s', collectionName, model);
      }

      self.db.dropCollection(collectionName, function (err, collection) {
        if (err) {
          debug('Error dropping collection %s for model %s: ', collectionName, model, err);
          if (!(err.name === 'MongoError' && err.ok === 0
            && err.errmsg === 'ns not found')) {
            // For errors other than 'ns not found' (collection doesn't exist)
            return modelCallback(err);
          }
        }
        // Recreate the collection
        if (self.debug) {
          debug('create collection %s for model %s', collectionName, model);
        }
        self.db.createCollection(collectionName, modelCallback);
      });
    }, function (err) {
      if (err) {
        return cb && cb(err);
      }
      self.autoupdate(models, cb);
    });
  } else {
    self.dataSource.once('connected', function () {
      self.automigrate(models, cb);
    });
  }
};

MongoDB.prototype.ping = function (cb) {
  var self = this;
  if (self.db) {
    this.db.collection('dummy').findOne({ _id: 1 }, cb);
  } else {
    self.dataSource.once('connected', function () {
      self.ping(cb);
    });
    self.dataSource.once('error', function (err) {
      cb(err);
    });
    self.connect(function () { });
  }
};

/**
 * Find a matching model instances by the filter or create a new instance
 *
 * Only supported on mongodb 2.6+
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} filter The filter
 * @param {Function} [callback] The callback function
 */
function optimizedFindOrCreate(model, filter, data, callback) {
  var self = this;
  if (self.debug) {
    debug('findOrCreate', model, filter, data);
  }

  var idValue = self.getIdValue(model, data);
  var idName = self.idName(model);

  if (idValue == null) {
    delete data[idName]; // Allow MongoDB to generate the id
  } else {
    var oid = self.coerceId(model, idValue); // Is it an Object ID?
    data._id = oid; // Set it to _id
    idName !== '_id' && delete data[idName];
  }

  filter = filter || {};
  var query = {};
  if (filter.where) {
    if (filter.where[idName]) {
      var id = filter.where[idName];
      delete filter.where[idName];
      id = self.coerceId(model, id);
      filter.where._id = id;
    }
    query = self.buildWhere(model, filter.where);
  }

  var sort = self.buildSort(model, filter.order);

  this.collection(model).findOneAndUpdate(
    query,
    { $setOnInsert: data },
    { projection: filter.fields, sort: sort, upsert: true },
    function (err, result) {
      if (self.debug) {
        debug('findOrCreate.callback', model, filter, err, result);
      }
      if (err) {
        return callback(err);
      }

      var value = result.value;
      var created = !!result.lastErrorObject.upserted;

      if (created && (value == null || Object.keys(value).length == 0)) {
        value = data;
        self.setIdValue(model, value, result.lastErrorObject.upserted);
      } else {
        value = self.fromDatabase(model, value);
        self.setIdValue(model, value, value._id);
      }

      value && idName !== '_id' && delete value._id;

      if (filter && filter.include) {
        self._models[model].model.include([value], filter.include, function (err, data) {
          callback(err, data[0], created);
        });
      } else {
        callback(null, value, created);
      }
    });
};

MongoDB.prototype.modifyFilter = function (filter) {
  var groupFilter = filter.group.groupBy ? filter.group.groupBy : [];
  var fieldsFilter = filter.fields || {};
  var fields;
  if (Array.isArray(fieldsFilter) && fieldsFilter.length > 0) {
    fields = [];
    fieldsFilter.forEach(function (prop) {
      if (groupFilter.indexOf(prop) !== -1) {
        fields.push(prop);
      }
    });
    filter.fields = fields;
  } else if (typeof fieldsFilter === 'object' && Object.keys(fieldsFilter).length > 0) {
    var included = [];
    var excluded = [];
    var keys = Object.keys(fieldsFilter);
    keys.forEach(function (k) {
      if (fieldsFilter[k]) {
        included.push(k);
      } else if ((k in fieldsFilter) && !fieldsFilter[k]) {
        excluded.push(k);
      }
    });
    if (included.length > 0) {
      fields = included;
    } else if (excluded.length > 0) {
      excluded.forEach(function (e) {
        var index = keys.indexOf(e);
        fields.splice(index, 1);
      });
    }
    filter.fields = fields;
  } else if (filter.group && filter.group.groupBy) {
    filter.fields = [];
    filter.group.groupBy.forEach(function (property) {
      filter.fields.push(property);
    });
  }

  if (filter.order) {
    var self = this;
    if (typeof filter.order === 'string') {
      filter.order = [filter.order];
    }
    filter.order.forEach(function (e) {
      var orderProp = e.split(/[\s,]+/);
      if (filter.group.groupBy && filter.group.groupBy.indexOf(orderProp[0]) === -1) {
        var index = filter.order.indexOf(e);
        filter.order.splice(index, 1);
      }
    });
    if (filter.order.length === 0) {
      delete filter.order;
    }
  }
}

MongoDB.prototype.buildPipeline = function (model, filter, query) {
  var pipeline = [];
  pipeline.push({ $match: query });
  var groupBy = filter.group.groupBy;
  if (groupBy) {
    var group = {};
    group['_id'] = {};
    groupBy.forEach(function (prop) {
      group['_id'][prop] = '$' + prop;
    });
    var aggrFn = filter.group;
    Object.keys(aggrFn).forEach(function (fnType) {
      fnType = fnType.toLowerCase();
      if (fnType !== 'groupby') {
        var fnVal = aggrFn[fnType];
        if (fnType !== 'count') {
          Object.keys(fnVal).forEach(function (prop) {
            var displayProp = fnVal[prop];
            group[displayProp] = {};
            group[displayProp]['$' + fnType] = '$' + prop;
          });
        } else {
          Object.keys(fnVal).forEach(function (prop) {
            var displayProp = fnVal[prop];
            group[displayProp] = {};
            group[displayProp]['$sum'] = 1;
          });
        }

      }
    });
    pipeline.push({ $group: group });
  }
  if (filter.group && filter.group.groupBy && filter.having) {
    var havingQuery = this.buildHavingClause(model, filter, filter.having);
    pipeline.push({ $match: havingQuery });
  }

  if (filter.order) {
    var order = {};
    var keys = filter.order;
    if (typeof keys === 'string') {
      keys = keys.split(',');
    }
    for (var index = 0, len = keys.length; index < len; index++) {
      var m = keys[index].match(/\s+(A|DE)SC$/);
      var key = keys[index];
      key = key.replace(/\s+(A|DE)SC$/, '').trim();
      if (filter.group.groupBy.indexOf(key) === -1) {
        if (m && m[1] === 'DE') {
          order[key] = -1;
        } else {
          order[key] = 1;
        }
      } else {
        if (m && m[1] === 'DE') {
          order["_id." + key] = -1;
        } else {
          order["_id." + key] = 1;
        }
      }
    }
    pipeline.push({ $sort: order });
  }

  if (filter.skip) {
    pipeline.push({ $skip: filter.skip });
  } else if (filter.offset) {
    pipeline.push({ $skip: filter.offset });
  }
  if (filter.limit) {
    pipeline.push({ $limit: filter.limit });
  }
  return pipeline;
}

/**
 * Transform the row data into a model data object
 * @param {string} model Model name
 * @param {object} rowData An object representing the row data from DB
 * @returns {object} Model data object
 */
MongoDB.prototype.fromRow = function (model, rowData) {
  if (rowData == null) {
    return rowData;
  }
  var data = Object.assign(rowData, rowData._id);
  delete data['_id'];
  return data;
};

/**
 * Execute a mongodb command
 * @param {String} model The model name
 * @param {String} command The command name
 * @param [...] params Parameters for the given command
 */
MongoDB.prototype.executeAggregation = function (model, command) {
  var collection = this.collection(model);
  // Get the parameters for the given command
  var args = [].slice.call(arguments, 2);
  // The last argument must be a callback function
  var callback = args[args.length - 1];
  var context = {
    model: model,
    collection: collection, req: {
      command: command,
      params: args
    }
  };
  args.pop();
  this.notifyObserversAround('execute', context, function (context, done) {
    debug('MongoDB: model=%s command=%s', model, command, args);
    callback(collection[command].apply(collection, args));
  });
};

MongoDB.prototype.buildHavingClause = function (model, filter, having) {
  var self = this;
  var query = {};
  if (having === null || (typeof having !== 'object')) {
    return query;
  }
  var idName = self.idName(model);
  Object.keys(having).forEach(function (k) {
    var cond = having[k];
    if (k === 'and' || k === 'or' || k === 'nor') {
      if (Array.isArray(cond)) {
        cond = cond.map(function (c) {
          return self.buildHavingClause(model, filter, c);
        });
      }
      query['$' + k] = cond;
      delete query[k];
      return;
    }
    var aggregation = having[k];
    for (var key in aggregation) {
      cond = having[k][key];
      var prop = self.getPropertyDefinition(model, key);
      var aggregationProp = filter.group[k] && filter.group[k][key];
      if (typeof aggregationProp === 'undefined')
        return;
      var spec = false;
      var options = null;
      if (cond && cond.constructor.name === 'Object') {
        options = cond.options;
        spec = Object.keys(cond)[0];
        cond = cond[spec];
      }
      if (spec) {
        if (spec === 'between') {
          query[aggregationProp] = { $gte: cond[0], $lte: cond[1] };
        } else if (spec === 'inq') {
          query[aggregationProp] = {
            $in: cond.map(function (x) {
              if ('string' !== typeof x || prop.type !== ObjectID) return x;
              return ObjectID(x);
            })
          };
        } else if (spec === 'nin') {
          query[aggregationProp] = {
            $nin: cond.map(function (x) {
              if ('string' !== typeof x || prop.type !== ObjectID) return x;
              return ObjectID(x);
            })
          };
        } else if (spec === 'like') {
          query[aggregationProp] = { $regex: new RegExp(cond, options) };
        } else if (spec === 'nlike') {
          query[aggregationProp] = { $not: new RegExp(cond, options) };
        } else if (spec === 'neq') {
          query[aggregationProp] = { $ne: cond };
        } else if (spec === 'regexp') {
          if (cond.global)
            console.warn('MongoDB regex syntax does not respect the `g` flag');

          query[aggregationProp] = { $regex: cond };
        }
        else {
          query[aggregationProp] = {};
          query[aggregationProp]['$' + spec] = cond;
        }
      } else {
        if (cond === null) {
          // http://docs.mongodb.org/manual/reference/operator/query/type/
          // Null: 10
          query[aggregationProp] = { $type: 10 };
        } else {
          query[aggregationProp] = cond;
        }
      }
    }
  });
  return query;
};

require('./lock')(MongoDB);
