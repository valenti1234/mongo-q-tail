const MongoTail = require("../lib/mongo-q-tail.js");

let options = {
    dbUrl: "mongodb://localhost:27017",
    dbName: 'tailtest',
    collection: 'q_submit',
    filter: { processed: false },
    throttle: 200,
    autoStart:true,
};

const mt = new MongoTail(options);

mt.on('data', function(data) {
    console.log('get a doc', data._id,data.name);
    mt.getQueue().then(res => {
        console.log('queue size', res);
        mt.updateDoc({
            filter: {_id: data._id},
            update: { processed: true }
        }).then(res => {
            mt.nextDoc();
        }).catch(console.error)

    }).catch(console.error)
})


function Generator() {
    const MongoClient = require('mongodb').MongoClient;
    const faker = require('faker');


    MongoClient.connect('mongodb://localhost:27017', { useNewUrlParser: true, useUnifiedTopology: true }, function(err, client) {
        if (err)
            throw err;

        let coll = client.db('tailtest').collection('q_submit'); //open collection


        setInterval(() => {

            var randomCard = faker.helpers.createCard(); // random contact card containing many properties

            let item = randomCard;
            item.processed = false;

            console.log('gen', item.name)
            coll.insertOne(item);

        }, 300)


    });

}

let gn = new Generator();
