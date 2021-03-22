const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const MongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017';
const dbName = 'sdc';
var lastProductId = {};

MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true }, (err, client) => {
  if (err) {
    return console.log('Error: unable to connect to MongoDB');
  }
  console.log(`Connected MongoDB: ${url}`);
  console.log(`Database: ${dbName}`);
  const db = client.db(dbName);
  const reviewsCollection = db.collection('reviews');
  reviewsCollection.drop();
  var count = 0;
  var operations = [];
  const mongoBulkWrite = async () => {
    await reviewsCollection.bulkWrite(operations, { ordered: true });
    count = 0;
    operations = [];
  };

  // ETL Process for reviews.csv
  (async () => {
    const parser = fs.createReadStream(path.resolve(__dirname, '../data/reviews.csv'), 'utf8').pipe(csv());
    for await (const record of parser) {
      var recommend, reported, response;
      if (count >= 10000) {
        await mongoBulkWrite();

        // const used = process.memoryUsage().heapUsed / 1024 / 1024;
        // console.log(`The script uses approximately ${Math.round(used * 100) / 100} MB`);
      }
      if (!lastProductId[record['product_id']]) {
        var product = { insertOne: {
          'product_id': Number(record['product_id']),
          // 'recommend': {
          //   'true': 0,
          //   'false': 0
          // },
          // 'characteristics': {
          //   'fit': {
          //     'id': 0,
          //     'value': 0
          //   },
          //   'length': {
          //     'id': 0,
          //     'value': 0
          //   },
          //   'comfort': {
          //     'id': 0,
          //     'value': 0
          //   },
          //   'quality': {
          //     'id': 0,
          //     'value': 0
          //   },
          //   'size': {
          //     'id': 0,
          //     'value': 0
          //   },
          //   'width': {
          //     'id': 0,
          //     'value': 0
          //   }
          // },
          // 'ratings': {
            //   '1': 0,
            //   '2': 0,
            //   '3': 0,
            //   '4': 0,
            //   '5': 0
            // },
          'characteristics': [],
          'reviews': []
        }}
        lastProductId = {};
        lastProductId[Number(record['product_id'])] = true;
        operations.push(product);
        count++;
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
          'photos': [],
          'characteristics': {}
        }}}
      }}
      operations.push(review);
      count++;
    }
    await mongoBulkWrite();
    console.log('Finished importing reviews.csv');
  })()
  .catch(err => {
    console.log('Error: cannot write reviews.csv to MongoDB', err);
  });

  // ETL Process for reviews_photos.csv
  (async () => {
    const parser = fs.createReadStream(path.resolve(__dirname, '../data/reviews_photos.csv'), 'utf8').pipe(csv());
    console.log('starting import of review photos...')
    for await (const record of parser) {
      if (count >= 10000) {
        await mongoBulkWrite();
      } else {
        var photo = { updateOne: {
          'filter': { reviews: { $elemMatch: { review_id: record['review_id']}} },
          'update': { $push: { photos: {
            'photo_id': Number(record['id']),
            'url': record['url']
          }}}
        }}
        operations.push(photo);
        count++;
      }
    }
    await mongoBulkWrite();
    console.log('Finished importing reviews_photos.csv');
  })()
  .catch(err => {
    console.log('Error: cannot write reviews_photos.csv to MongoDB', err);
  });

  // ETL Process for characteristics.csv
  (async () => {
    const parser = fs.createReadStream(path.resolve(__dirname, '../data/characteristics.csv'), 'utf8').pipe(csv());
    console.log('starting import of characteristics...')
    for await (const record of parser) {
      if (count >= 10000) {
        await mongoBulkWrite();
      } else {
        var characteristic = { updateOne: {
          'filter': { product_id: record['product_id']},
          'update': { $push: { characteristics: {
            'characteristic_id': Number(record['id']),
            'name': record['name']
          }}}
        }}
        operations.push(characteristic);
        count++;
      }
    }
    await mongoBulkWrite();
    console.log('Finished importing characteristics.csv');
  })()
  .catch(err => {
    console.log('Error: cannot write characteristics.csv to MongoDB', err);
  });

  // ETL Process for characteristic_reviews.csv
  (async () => {
    const parser = fs.createReadStream(path.resolve(__dirname, '../data/characteristic_reviews.csv'), 'utf8').pipe(csv());
    console.log('starting import of characteristic_reviews...')
    for await (const record of parser) {
      if (count >= 10000) {
        await mongoBulkWrite();
      } else {
        var characteristic = { updateOne: {
          'filter': { reviews: { $elemMatch: { review_id: record['review_id']}}},
          'update': { $set: { characteristics: {
            'characteristic_id': Number(record['characteristic_id']),
            'value': Number(record['value'])
          }}}
        }}
        operations.push(characteristic);
        count++;
      }
    }
    await mongoBulkWrite();
    console.log('Finished importing characteristic_reviews.csv');
  })()
  .catch(err => {
    console.log('Error: cannot write characteristic_reviews.csv to MongoDB', err);
  });
});

// ETL Update Template:
/*
  (async () => {
    const parser = fs.createReadStream(path.resolve(__dirname, '../data/XXX'), 'utf8').pipe(csv());
    for await (const record of parser) {
      if (count >= 10000) {
        await mongoBulkWrite();
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
    await mongoBulkWrite();
    console.log('Finished importing XXX');
  })()
  .catch(err => {
    console.log('Error: cannot write XXX to MongoDB', err);
  })
*/

// db.reviews.updateOne( { reviews: { $elemMatch: { review_id: 5}}}, { $push: { photos: { "photo_id": 2, "url": "https://images.unsplash.com/photo-1561693532-9ff59442a7db?ixlib=rb-1.2.1&auto=format&fit=crop&w=975&q=80" }}})

    //       var photo = { updateOne: {
    //         'filter': { reviews: { $elemMatch: { review_id: record['review_id']}} },
    //         'update': { $push: { photos: {
    //           'photo_id': Number(record['id']),
    //           'url': record['url']
    //         }}}
    //       }}
