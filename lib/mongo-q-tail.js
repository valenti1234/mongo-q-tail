'use strict';
const MongoClient = require('mongodb').MongoClient;
const EventEmitter = require('events').EventEmitter;
const util = require('util');

const MongoTail = function(options) {

    /* options:
        prefix : string, dbg log prefix
        autoStart : bool , wait for docs auto on constructor
        size : capped size when creater collection
        filter: query filter
        collection: name of collection
        dbUrl: database url
        dbName: database name
        throttle: ms wait time before consuming another doc
    
    */

    let prefix = '_MongoTail';
    const dbg = function(obj, ...argumentArray) {
        argumentArray.unshift(obj);
        argumentArray.unshift('[' + prefix + ']');
        return console.log.apply(this, argumentArray);
    };
    const dbgE = function(obj, ...argumentArray) {
        argumentArray.unshift(obj);
        argumentArray.unshift('[' + prefix + ']');
        return console.error.apply(this, argumentArray);
    };


    if (options.prefix) {
        prefix = options.prefix;
    }

    const _this = this;
    EventEmitter.call(_this);

    _this.init = () => {
        return new Promise(function(fulfilled, rejected) {
            _this.options = options;
            _this.options.size = options.size || 1073741824; //default 1G
            _this.cursorList = [];
            _this.connectDb().then(res => {
                _this.createCollection().then(res => {
                    if (_this.options.autoStart) {
                        _this.start();
                    }
                }).catch(dbgE);
            }).catch(dbgE);
        });
    };

    _this.start = () => {
     //   dbg(56,'start');
        _this.waitDoc();
    }

    _this.createCollection = () => {
        return new Promise(function(fulfilled, rejected) {
            if (_this.listCollections.indexOf(_this.options.collection) < 0) {
                //   dbg(32, 'createCollection');
                _this.dbConn.createCollection(_this.options.collection, { capped: true, size: _this.options.size }) //1G capped
                    .then(res => {
                        fulfilled(0);
                    }).catch(dbgE);
            }
            else {
                fulfilled(0);
            }
        });
    };

    _this.connectDb = () => {
        return new Promise(function(fulfilled, rejected) {
            MongoClient.connect(_this.options.dbUrl, { useNewUrlParser: true, useUnifiedTopology: true }, function(err, client) {
                if (err) {
                    return rejected(err);
                }

                _this.dbConn = client.db(_this.options.dbName); //open DB
                _this.dbConn.listCollections().toArray(function(err, collInfos) {
                    if (err) {
                        return rejected(err);
                    }
                    _this.listCollections = [];
                    for (let c of collInfos) {
                        _this.listCollections.push(c.name);
                    }
                    return fulfilled(0);
                });
            });
        });
    };

    _this.insertDoc = (ctx) => {
        return new Promise(function(fulfilled, rejected) {
            let coll = _this.dbConn.collection(_this.options.collection);
            ctx.processed = false;
            coll.insertOne(ctx).then(res => {
                if (res) {
                    fulfilled(res);
                }
            }).catch(rejected);
        });
    };

    _this.updateDoc = (ctx) => {
        return new Promise(function(fulfilled, rejected) {
            let coll = _this.dbConn.collection(_this.options.collection);
            //     dbg(93, 'updateDoc', coll.namespace, ctx.filter);
            coll.findOneAndUpdate(ctx.filter, { $set: ctx.update }, { upsert: false }).then(res => {
                if (res) {
                    fulfilled(res);
                }
            }).catch(rejected);
        });
    };

    _this.getQueue = () => {
        return new Promise(function(fulfilled, rejected) {
            let filter = _this.options.filter;
            let coll = _this.dbConn.collection(_this.options.collection);
            let cursor = coll.find(filter);
            cursor.count().then(res => {
                return fulfilled(res);
            }).catch(rejected);
        });
    };

    _this.nextDoc = () => {
        if (_this.options.throttle) {
            setTimeout(() => {
                // dbg(116, 'resuming');
                _this.cursorStream.resume();
            }, _this.options.throttle);
        }
        else {
            //            dbg(121, 'resuming');
            _this.cursorStream.resume();
        }
    };

    _this.waitDoc = (ctx) => {

        let coll = _this.dbConn.collection(_this.options.collection);

        function waitTail() {
            let filter = _this.options.filter;
            dbg('mongo-q-tail wait for', filter, coll.namespace);
            let cursor = coll.find(filter, { tailable: true, awaitdata: true });
            _this.cursorList.push(cursor);
            _this.cursorStream = cursor.stream();
            _this.cursorStream.on('data', function(item) {
                _this.cursorStream.pause();
                _this.emit('data', item);
            });
        }

        function waitOneDoc(notif) {
            if (_this.isDestroyed) {
                return;
            }
            let filter = _this.options.filter;
            let cursor = coll.find(filter, { tailable: true, awaitdata: true });
            cursor.count().then(res => {
                if (res < 1) {
                    if (notif)
                        dbg('waitOneDoc no docs', filter, coll.namespace);
                    return setTimeout(waitOneDoc, 2000)
                }
                else { waitTail(); }
            }).catch(dbgE)
        }
        waitOneDoc(true);
    }
    
    
    
    _this.init();
}

util.inherits(MongoTail, EventEmitter);
module.exports = MongoTail;
