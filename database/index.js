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
var totalCount = 0;
var operations = [];
var reviewPhotos = [];
var lastProductId = '';
var lastReviewId = '';

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
// app.use(express.static(path.resolve(__dirname, '../legacy-fec-codebase/frontend-capstone/dist/index.html')));

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
    reviewsCollection.createIndex({ product_id: 1 });
    reviewsCollection.createIndex({ 'reviews.review_id': 1 });

    const mongoBulkWrite = async (type) => {
      for (const key in csvData) {
        if (type === 'insert') {
          operations.push({ insertOne: csvData[key]});
        } else if (type === 'update') {
          operations.push({ updateOne: csvData[key]});
        }
      }
      await reviewsCollection.bulkWrite(operations, { ordered: true });
      count = 0;
      operations = [];
      csvData = {};
    };

    // ETL Process for reviews.csv
    (async () => {
      const parser = fs.createReadStream(path.resolve(__dirname, '../data/reviews.csv'), 'utf8').pipe(csv());
      console.log('Starting import of reviews...');
      for await (const record of parser) {
        var recommend, reported, response, productId = record['product_id'];
        if (count >= 10000 && productId !== lastProductId) {
          await mongoBulkWrite('insert');
          console.log(`Imported ${totalCount.toLocaleString()} reviews out of 5,777,922 total reviews...`);
        }
        if (!csvData[productId]) {
          var product = {
            'product_id': Number(productId),
            'characteristics': [],
            'reviews': []
          }
          csvData[productId] = product;
          count++;
          lastProductId = productId;
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
        csvData[productId]['reviews'].push(review);
        totalCount++;
      }
      await mongoBulkWrite('insert');
      console.log('Finished importing reviews.csv!');
      totalCount = 0;
    })()
    .catch(err => {
      console.log('Error: cannot write reviews.csv to MongoDB', err);
    })
    // ETL Process for reviews_photos.csv
    .then(results => {
      (async () => {
        const parser = fs.createReadStream(path.resolve(__dirname, '../data/reviews_photos.csv'), 'utf8').pipe(csv());
        console.log('Starting import of review photos...');
        for await (const record of parser) {
          var reviewId = record['review_id'];
          if (Number(reviewId) > Number(lastReviewId)) {
            var photos = {
              'filter': { 'reviews.review_id': Number(lastReviewId) },
              'update': { $addToSet: { 'reviews.$.photos': { $each: reviewPhotos } } },
              'hint': { 'reviews.review_id': 1 }
            }
            csvData[lastReviewId] = photos;
            count++;
            reviewPhotos = [];
          }
          if (count >= 10000 && reviewId !== lastReviewId) {
            await mongoBulkWrite('update');
            console.log(`Imported ${totalCount.toLocaleString()} photos out of 2,742,832 total photos...`);
          }
          if (!csvData[reviewId]) {
            lastReviewId = reviewId;
            csvData[reviewId] = {};
          }
          var photo = {
            'photo_id': Number(record['id']),
            'url': record['url']
          }
          reviewPhotos.push(photo);
          totalCount++;
        }
        var photos = {
          'filter': { 'reviews.review_id': Number(lastReviewId) },
          'update': { $addToSet: { 'reviews.$.photos': { $each: reviewPhotos } } },
          'hint': { 'reviews.review_id': 1 }
        }
        csvData[lastReviewId] = photos;
        await mongoBulkWrite('update');
        console.log('Finished importing reviews_photos.csv!');
        totalCount = 0;
      })()
      .catch(err => {
        console.log('Error: cannot write reviews_photos.csv to MongoDB', err);
      });
    })
  });
});

// Server Routes
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
