var async = require('async');

module.exports = mixinLock;

var transactionsMap = {};

var lockCollectionName = 'Lock';

var locStatus = 'LOCK';
var unlocStatus = 'UNLOCK';

/*!
 * @param {PostgreSQL} PostgreSQL connector class
 */
function mixinLock(MongoDb) {

    MongoDb.prototype.acquire = function(modelInstance, options, cb) {
        var self = this;
        var tryAcquire = function(asyncCallback) {
            var collection = self.db.collection(lockCollectionName);
            var filter = {modelId: modelInstance.id, modelName: modelInstance._type,lockStatus: unlocStatus};
            var update = {modelId: modelInstance.id, modelName: modelInstance._type,lockStatus: locStatus};
            var ops = {upsert: true, returnOriginal: false};
            collection.findOneAndUpdate(filter, update, ops, function (err, result) {
                if (err) {
                    return asyncCallback(err);
                } else {
                    return asyncCallback(null, result);
                }
            });
        };

        async.retry(10, tryAcquire, function(err) {
            if (err) {
                return cb(err);
            } else {
                return cb(null);
            }
        });
    };

    MongoDb.prototype.release = function(err, modelInstance, releaseLockCb, valid) {
        var self = this;
        var tryRelease = function(asyncCallback) {
            var collection = self.db.collection(lockCollectionName);
            var filter = {modelId: modelInstance.id, modelName: modelInstance._type,lockStatus: locStatus};
            var update = {modelId: modelInstance.id, modelName: modelInstance._type,lockStatus: unlocStatus};
            var ops = {upsert: true, returnOriginal: false};
            collection.findOneAndUpdate(filter, update, ops, function (err, result) {
                if (err) {
                    return asyncCallback(err);
                } else {
                    return asyncCallback(null, result);
                }
            });
        };

        async.retry(10, tryRelease, function(asyncErr) {
            if (asyncErr) {
                return releaseLockCb(asyncErr, valid);
            } else {
                if (err) {
                    return releaseLockCb(err, valid);
                } else {
                    return releaseLockCb(null, valid);
                }
            }
        });
    };
}
