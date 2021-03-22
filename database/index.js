const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const MongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017';

const dbName = 'sdc';
var count = 0;
var operations = [];

MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true }, (err, client) => {
  if (err) {
    return console.log('Error: unable to connect to MongoDB');
  }
  console.log(`Connected MongoDB: ${url}`);
  console.log(`Database: ${dbName}`);
  const db = client.db(dbName);
  const reviewsCollection = db.collection('reviews');
  reviewsCollection.drop();


  // use csv-parser
  // dont need product id or name
  // fs.createReadStream (for each csv file!!) => repeat this step
  // then pipe into parser
  // inside for await loop:
  // clean data
  // group by productid
  // update 10k records at once (look at Mongo upsert command but slower than insert) => bulkWrite? (create variable to store # of records you've gotten, meet limit then execute, then clear variables)

  // db.listCollections({ name: 'reviews' })
  //   .next((err, collectionInfo) => {
  //     if (collectionInfo) {
  //       reviewsCollection.drop();
  //     } else {
  //       db.createCollection('reviews');
  //     }
  //   });

  (async () => {
    const parser = fs.createReadStream(path.resolve(__dirname, '../data/reviews.csv'), 'utf8').pipe(csv())
    const mongoBulkWrite = () => {
      reviewsCollection.bulkWrite(operations, { ordered: true });
      operations = [];
      count = 0;
    };
    for await (const record of parser) {
      if (count >= 10000) {
        mongoBulkWrite();
      } else {
        if (record['recommend'] === 'true' || record['recommend'] === 'false') {
          if (record['recommend'] === 'true') {
            var recommend = 1;
          } else {
            var recommend = 0;
          }
        }
        if (record['reported'] === 'true' || record['reported'] === 'false') {
          if (record['reported'] === 'true') {
            var reported = 1;
          } else {
            var reported = 0;
          }
        }
        if (record['response'] === '') {
          var response = null;
        }
        var document = { insertOne: {
          'id': Number(record['id']),
          'product_id': Number(record['product_id']),
          'rating': Number(record['rating']),
          'date': record['date'],
          'summary': record['summary'],
          'body': record['body'],
          'recommend': recommend || Number(record['recommend']),
          'reported': reported || Number(record['reported']),
          'reviewer_name': record['reviewer_name'],
          'reviewer_email': record['reviewer_email'],
          'response': response || record['response'],
          'helpfulness': Number(record['helpfulness'])
        }}
        operations.push(document);
        count++;
      }
    }
    mongoBulkWrite();
    console.log('Finished importing reviews');
  })()
  .catch(err => {
    console.log('Error: cannot write to MongoDB', err);
  })

  // after finishing operations, client.close()
});

