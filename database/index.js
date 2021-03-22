const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const MongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017';

const dbName = 'sdc';
var count = 0;
var operations = [];
var productIds = {};

MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true }, (err, client) => {
  if (err) {
    return console.log('Error: unable to connect to MongoDB');
  }
  console.log(`Connected MongoDB: ${url}`);
  console.log(`Database: ${dbName}`);
  const db = client.db(dbName);
  const reviewsCollection = db.collection('reviews');
  reviewsCollection.drop();
  const mongoBulkWrite = () => {
    reviewsCollection.bulkWrite(operations, { ordered: true });
    operations = [];
    count = 0;
  };

  (async () => {
    const parser = fs.createReadStream(path.resolve(__dirname, '../data/reviews.csv'), 'utf8').pipe(csv());
    for await (const record of parser) {
      var recommend, reported, response;
      if (count >= 10000) {
        mongoBulkWrite();
      }

      // check if document with current product id has already been created
      if (!productIds[record['product_id']]) {
        var product = { insertOne: {
          'product_id': Number(record['product_id']),
          'recommend': {
            'true': 0,
            'false': 0
          },
          'characteristics': {
            'fit': {
              'id': 0,
              'value': 0
            },
            'length': {
              'id': 0,
              'value': 0
            },
            'comfort': {
              'id': 0,
              'value': 0
            },
            'quality': {
              'id': 0,
              'value': 0
            },
            'size': {
              'id': 0,
              'value': 0
            },
            'width': {
              'id': 0,
              'value': 0
            }
          },
          'ratings': {
            '1': 0,
            '2': 0,
            '3': 0,
            '4': 0,
            '5': 0
          },
          'reviews': []
        }}
        productIds[Number(record['product_id'])] = true;
        operations.push(product);
      }

      if (record['recommend'].includes('true') || record['recommend'].includes('1')) {
        recommend = true;
      } else {
        recommend = false;
      }

      if (record['reported'].includes('true') || record['reported'].includes('1')) {
        reported = true;
      } else {
        reported = false;
      }

      if (record['response'] === '') {
        response = null;
      } else {
        response = record['response'];
      }

      var review = { updateOne: {
        'filter': { product_id: Number(record['product_id']) },
        'update': { $push: { reviews: {
          'review_id': Number(record['id']),
          'rating': Number(record['rating']),
          'date': record['date'],
          'summary': record['summary'],
          'body': record['body'],
          'recommend': recommend,
          'reported': reported,
          'reviewer_name': record['reviewer_name'],
          'reviewer_email': record['reviewer_email'],
          'response': response,
          'helpfulness': Number(record['helpfulness']),
          'photos': []
        }}}
      }}
      operations.push(review);
      count++;
    }
    mongoBulkWrite();
    console.log('Finished importing reviews.csv');
  })()
  .catch(err => {
    console.log('Error: cannot write reviews.csv to MongoDB', err);
  })

});

// ETL Update Template:
/*
  (async () => {
    const parser = fs.createReadStream(path.resolve(__dirname, '../data/XXX'), 'utf8').pipe(csv());
    for await (const record of parser) {
      if (count >= 10000) {
        mongoBulkWrite();
      } else {
        var document = { updateOne: {
          'filter': { product_id: Number(record['product_id']) },
          'update': { $push: { reviews: {
            'review_id': Number(record['id'])
          }}}
        }}
        operations.push(document);
        count++;
      }
    }
    mongoBulkWrite();
    console.log('Finished importing XXX');
  })()
  .catch(err => {
    console.log('Error: cannot write XXX to MongoDB', err);
  })
*/

