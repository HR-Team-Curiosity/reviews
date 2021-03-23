const express = require('express');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const bodyParser = require('body-parser');
const MongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017';
const app = express();
const port = 8080;
const dbName = 'sdc';
var csvData = {};
var count = 0;
var operations = [];
var lastProductId = '';

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
// app.use(express.static(path.resolve(__dirname, '../legacy-fec-codebase/frontend-capstone/dist/index.html')));

// app.get('/', (req, res) => {
//   res.sendFile(path.resolve(__dirname, '../legacy-fec-codebase/frontend-capstone/dist/index.html'));
// });

// // getReviewMetaData route
// app.get('/reviews/:id/meta', (req, res) => {
//   var params = req.params.id;
// });

// // getReviewsOfProduct route
// app.get('/reviews/:id/list?sort=:sortString:asc&count=:count}', (req, res) => {
//   var params = [req.params.id, req.params.sortString, req.params.count];
//   reviewsCollection.find({ product_id: params[0], reviews: {qty: params[2]}}).limit(5);
// });

app.listen(port, () => {
  console.log(`Express server listening at http://localhost:${port}`);

  MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true }, (err, client) => {
    if (err) {
      return console.log('Error: unable to connect to MongoDB');
    }
    console.log(`Connected MongoDB: ${url}`);
    console.log(`Database: ${dbName}`);
    const db = client.db(dbName);
    const reviewsCollection = db.collection('reviews');
    reviewsCollection.drop();

    const mongoBulkWrite = async () => {
      for (const key in csvData) {
        operations.push({ insertOne: csvData[key]});
      }
      await reviewsCollection.bulkWrite(operations, { ordered: true });
      count = 0;
      operations = [];
      csvData = {};
    };

    // ETL Process for reviews.csv
    (async () => {
      const parser = fs.createReadStream(path.resolve(__dirname, '../data/reviews.csv'), 'utf8').pipe(csv());
      for await (const record of parser) {
        var recommend, reported, response;
        if (count >= 10000 && record['product_id'] !== lastProductId) {
          await mongoBulkWrite();
        }
        if (!csvData[record['product_id']]) {
          var productId = record['product_id'];
          var product = {
            'product_id': Number(record['product_id']),
            'characteristics': [],
            'reviews': []
          }
          csvData[record['product_id']] = product;
          count++;
          lastProductId = record['product_id'];
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
        var review = {
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
        }
        var productId = record['product_id'];
        csvData[productId]['reviews'].push(review);
      }
      await mongoBulkWrite();
      console.log('Finished importing reviews.csv');
    })()
    .catch(err => {
      console.log('Error: cannot write reviews.csv to MongoDB', err);
    });
    // .then(results => {
    //   // ETL Process for reviews_photos.csv
    //   (async () => {
    //     const parser = fs.createReadStream(path.resolve(__dirname, '../data/reviews_photos.csv'), 'utf8').pipe(csv());
    //     console.log('starting import of review photos...')
    //     for await (const record of parser) {
    //       if (count >= 10000) {
    //         await mongoBulkWrite();
    //       } else {
    //         var photo = { updateOne: {
    //           'filter': { reviews: { $elemMatch: { review_id: record['review_id']}} },
    //           'update': { $push: { photos: {
    //             'photo_id': Number(record['id']),
    //             'url': record['url']
    //           }}}
    //         }}
    //         operations.push(photo);
    //         count++;
    //       }
    //     }
    //     await mongoBulkWrite();
    //     console.log('Finished importing reviews_photos.csv');
    //   })()
    //   .catch(err => {
    //     console.log('Error: cannot write reviews_photos.csv to MongoDB', err);
    //   })
    //   .then(results => {
    //     // ETL Process for characteristics.csv
    //     (async () => {
    //       const parser = fs.createReadStream(path.resolve(__dirname, '../data/characteristics.csv'), 'utf8').pipe(csv());
    //       console.log('starting import of characteristics...')
    //       for await (const record of parser) {
    //         if (count >= 10000) {
    //           await mongoBulkWrite();
    //         } else {
    //           var characteristic = { updateOne: {
    //             'filter': { product_id: record['product_id']},
    //             'update': { $push: { characteristics: {
    //               'characteristic_id': Number(record['id']),
    //               'name': record['name']
    //             }}}
    //           }}
    //           operations.push(characteristic);
    //           count++;
    //         }
    //       }
    //       await mongoBulkWrite();
    //       console.log('Finished importing characteristics.csv');
    //     })()
    //     .catch(err => {
    //       console.log('Error: cannot write characteristics.csv to MongoDB', err);
    //     })
    //     .then(results => {
    //       // ETL Process for characteristic_reviews.csv
    //       (async () => {
    //         const parser = fs.createReadStream(path.resolve(__dirname, '../data/characteristic_reviews.csv'), 'utf8').pipe(csv());
    //         console.log('starting import of characteristic_reviews...')
    //         for await (const record of parser) {
    //           if (count >= 10000) {
    //             await mongoBulkWrite();
    //           } else {
    //             var characteristic = { updateOne: {
    //               'filter': { reviews: { $elemMatch: { review_id: record['review_id']}}},
    //               'update': { $set: { characteristics: {
    //                 'characteristic_id': Number(record['characteristic_id']),
    //                 'value': Number(record['value'])
    //               }}}
    //             }}
    //             operations.push(characteristic);
    //             count++;
    //           }
    //         }
    //         await mongoBulkWrite();
    //         console.log('Finished importing characteristic_reviews.csv');
    //       })()
    //       .catch(err => {
    //         console.log('Error: cannot write characteristic_reviews.csv to MongoDB', err);
    //       });
    //     })
    //   })
    // })
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
