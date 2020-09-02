  const faker = require('faker');

const MongoTail = require("../lib/mongo-q-tail.js");
//const dbUrl = 'mongodb://mongodb_host:27017'
//const dbUrl = 'mongodb://172.16.14.136:27017'

//const dbUrl = 'mongodb://172.16.14.136:27017';
//const dbUrl  = "mongodb://172.16.14.136:27017,//172.16.12.136:27017?replicaSet=rs0";
//const dbUrl  = 'mongodb://172.16.12.136:27017?replicaSet=rs0&slaveOk=true';

//const dbUrl  = 'mongodb://172.16.14.136:27017?replicaSet=rs0';
//const dbUrl  = 'mongodb://repl12:27017,repl14:27017';

//-------------
//const dbUrl  = 'mongodb://repl12:27017?replicaSet=rs0';
const dbUrl = 'mongodb://mongo-tail:27017';

function startTail() {
    let options = {
        dbUrl: dbUrl,
        dbName: 'tailtest',
        collection: 'q_submit',
        filter: { processed: false },
        processedUpdate: { processed: true },
        throttle: 30,
        autoStart: true,
        alertCount: 10,
        debugInfo: true,
        indexList: ["processed", "address.zipcode", "accountHistory.type"],
        size: 10240000,
        drop: true,

    };

    const mt = new MongoTail(options);

    mt.on('error', function(data) {
        console.error('Event error', data);
    })
    
    mt.on('start', function(data) {
        console.log('MongoTail started', data);

        function insertOne() {
            return new Promise(function(fulfilled, rejected) {
                var randomCard = faker.helpers.createCard(); // random contact card containing many properties
                let item = randomCard;
                item.processed = false;
                console.log('gen', item.name)
                mt.insertDoc(item).then(fulfilled).catch(rejected);
            });
        }

        function loopInsert() {
            let high = 118;
            let low = 1;

            setTimeout(() => {
                insertOne().then(loopInsert).catch(err => {
                    console.error('insertOne error', err);
                });
            }, Math.random() * (high - low) + low);
        }

        setTimeout(loopInsert,10*1000);

    })

    mt.on('data', function(data) {
        console.log('get a doc', data._id, data.name);
        mt.markAsProcessed({
            filter: { _id: data._id }
        }).then(res => {
            mt.nextDoc();
        }).catch(console.error)
    })

    mt.on('alert', function(data) {
        console.log('get an alert', data);
        mt.options.throttle = 1;
        setTimeout(() => {
            mt.options.throttle = 1000;
        }, 10 * 1000)
    })



}


setTimeout(() => {
    startTail();
}, 1 * 1000)
