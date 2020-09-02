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
        timeoutEmpty: sleep time when empty collection
        processedUpdate: update obj
        indexList: array of indexes to use
        alertCount: > 1 set an alerting event on queue 
        drop: true = drop collection when start
        debugInfo: true = show debug
    
    */
    const _this = this;

    let debugInfo = false
    let prefix = '_MongoTail';
    const dbg = function(obj, ...argumentArray) {
        if (!debugInfo) {
            return;
        }
        argumentArray.unshift(obj);
        argumentArray.unshift('[' + prefix + ']');
        return console.log.apply(this, argumentArray);
    };
    const dbgE = function(obj, ...argumentArray) {
        if (!debugInfo) {
            return;
        }
        argumentArray.unshift(obj);
        argumentArray.unshift('[' + prefix + ']');
        return console.error.apply(this, argumentArray);
    };

    debugInfo = options.debugInfo;

    if (options.prefix) {
        prefix = options.prefix;
    }


    EventEmitter.call(_this);
    _this.timeoutEmpty = options.timeoutEmpty || 8000;

    _this.init = () => {
        return new Promise(function(fulfilled, rejected) {
            _this.options = options;
            _this.options.size = options.size || 1073741824; //default 1G
            _this.cursorList = [];
            _this.connectDb().then(res => {

                _this.createCollection().then(res => {
                    dbg(64, res)
                    _this.checkIndex().then(() => {
                        if (_this.options.autoStart) {
                            setTimeout(_this.start, 0)
                            //_this.start();
                        }
                    });
                }).catch(err => {
                    dbgE('createCollection', err);
                    return rejected(err);
                });
            }).catch(dbgE);
        });
    };

    _this.startAlertCount = () => {
        setInterval(res => {
            dbg('AlertCount');
            let filter = _this.options.filter;
            let coll = _this.dbConn.collection(_this.options.collection);
            let cursor = coll.find(filter);
            cursor.count().then(res => {
                if (res > _this.options.alertCount) {
                    _this.emit('alert', { type: 'queue', value: res });
                }
            }).catch(dbgE);
        }, 30 * 1000)
    }

    _this.start = () => {
        //   dbg(56,'start');
        if (_this.options.alertCount) {
            setTimeout(_this.startAlertCount, 1000 * 30)
        }

        _this.emit('start', {});

        _this.waitDoc();
    };


    _this.checkIndex = () => {
        return new Promise(function(fulfilled, rejected) {
            if (!_this.options.indexList || _this.options.indexList.length < 1) {
                return fulfilled(0);
            }

            let coll = _this.dbConn.collection(_this.options.collection);
            for (let i of _this.options.indexList) {
                dbg(98, 'createIndex', i);
                coll.createIndex(i).then(dbg).catch(dbgE);
            }
            return fulfilled(0);
        });
    };

    _this.info = () => {
        return new Promise(function(fulfilled, rejected) {
            _this.dbConn.collection(_this.options.collection).stats(function(err, stats) {
                if (err) {
                    dbg('info', err);
                    return rejected(err);
                }
                return fulfilled(stats);
            })

        })
    }

    _this.createCollection = () => {
        return new Promise(function(fulfilled, rejected) {
            function createIt() {
                dbg(107, 'createIt', _this.options.collection);
                _this.dbConn.createCollection(_this.options.collection, { capped: true, size: _this.options.size }) //1G capped
                    .then(res => {
                        fulfilled(0);
                    }).catch(err => {
                        dbgE('createCollection', err);
                    });
            }

            if (_this.listCollections.indexOf(_this.options.collection) < 0) {
                return createIt();
            }
            else {
                if (_this.options.drop) {
                    dbg(115, 'drop collection', _this.options.collection);
                    _this.dbConn.collection(_this.options.collection).drop().then(createIt).catch(err => {
                        dbgE('drop', err);
                        return rejected(err)
                    });
                }
                else {
                    _this.info().then(res => {
                        dbg(152, res.capped);
                        if (!res.capped) {
                            let err = 'collectionis not capped cannot proceed';
                            dbg(147, err);

                            return rejected(err);
                        }

                    }).catch(dbgE);
                    return fulfilled(0);
                }


            }
        });
    };

    _this.connectDb = () => {
        return new Promise(function(fulfilled, rejected) {

            let connect_options = {
                useNewUrlParser: true,
                useUnifiedTopology: true,

                /*
                server: {
                    socketOptions: {
                        autoReconnect: true,
                        keepAlive: 1,
                        connectTimeoutMS: 30000,
                        socketTimeoutMS: 0
                    }
                },
                replSet: {
                    socketOptions: {
                        keepAlive: 1,
                        connectTimeoutMS: 30000,
                        socketTimeoutMS: 0
                    }
                }
                */
            }

            MongoClient.connect(_this.options.dbUrl, connect_options, function(err, client) {
                if (err) {
                    return rejected(err);
                }
                dbg(202, client.s.options);
                _this.dbConn = client.db(_this.options.dbName); //open DB



                _this.dbConn.listCollections().toArray(function(err, collInfos) {
                    if (err) {
                        return rejected(err);
                    }
                    _this.listCollections = [];
                    for (let c of collInfos) {
                        _this.listCollections.push(c.name);
                    }
                    dbg(151, _this.listCollections);
                    return fulfilled(0);
                });
            });
        });
    };

    _this.processError = (ctx) => {
        dbgE(ctx.desc, ctx.error);
        _this.emit('error', ctx);

    }

    _this.insertDoc = (ctx) => {
        return new Promise(function(fulfilled, rejected) {
            let coll = _this.dbConn.collection(_this.options.collection);
            ctx.processed = false;
            coll.insertOne(ctx).then(res => {
                if (res) {
                    fulfilled(res);
                }
            }).catch(err => {
                _this.processError({ desc: 'insertOne', error: err });
                return rejected(err);
            });
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

    _this.markAsProcessed = (ctx) => {
        return new Promise(function(fulfilled, rejected) {
            if (_this.options.processedUpdate) {
                let coll = _this.dbConn.collection(_this.options.collection);
                //     dbg(93, 'updateDoc', coll.namespace, ctx.filter);
                coll.findOneAndUpdate(ctx.filter, { $set: _this.options.processedUpdate }, { upsert: false }).then(res => {
                    if (res) {
                        return fulfilled(res);
                    }
                }).catch(rejected);
            }
            else {
                let err = 'cannot markAsProcessed update is null';
                return rejected(err);
            }
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

    _this.pause = () => {
        return _this.cursorStream.pause();
    }

    _this.resume = () => {
        return _this.cursorStream.resume();
    }

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
                //    dbg(156, 'got a new doc', coll.namespace, item._id);
                _this.cursorStream.pause();
                _this.emit('data', item);
            });
        }

        function waitOneDoc(notif) {
            if (_this.isDestroyed) {
                dbgE('isDestroyed', _this.isDestroyed)
                return;
            }
            let filter = _this.options.filter;
            let cursor = coll.find(filter, { tailable: true, awaitdata: true });
            cursor.count().then(res => {
                  dbgE('waitOneDoc',filter,res);
                if (res < 1) {
                    if (notif)
                        dbg('waitOneDoc no docs', filter, coll.namespace);
                    return  _this.waitDocTimer = setTimeout(waitOneDoc, _this.timeoutEmpty);
                    //return setTimeout(waitOneDoc, _this.timeoutEmpty)
                }
                else { waitTail(); }
            }).catch(dbgE)
        }
        waitOneDoc(true);


    }

    _this.destroy = ()=>{
        dbg(350,'destroy',_this.cursorList);
        clearTimeout(_this.waitDocTimer);
        _this.cursorList.forEach(c=>{
            dbg('close cursor',c);
            c.close();
        })
        _this.isDestroyed = true;
    }

    _this.init().then(dbg).catch(dbgE);
}

util.inherits(MongoTail, EventEmitter);
module.exports = MongoTail;
