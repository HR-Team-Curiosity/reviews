const path = require('path');
const express = require('express');
const bodyParser = require('body-parser');
const url = 'mongodb://localhost:27017';
const MongoClient = require('mongodb').MongoClient;
const dbName = 'sdc';
const app = express();
const port = 8080;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
// app.use(express.static(path.resolve(__dirname, '../legacy-fec-codebase/frontend-capstone/dist/index.html')));

// manually run index.js to start ETL process -- npm script? or run in here?

MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true }, (err, client) => {
  if (err) {
    return console.log('Error: unable to connect to MongoDB');
  }
  console.log(`Connected MongoDB: ${url}`);
  console.log(`Database: ${dbName}`);
  const db = client.db(dbName);
  const reviewsCollection = db.collection('reviews');

  app.listen(port, () => {
    console.log(`Express server listening at http://localhost:${port}`);
  });

  app.get('/', (req, res) => {
    res.sendFile(path.resolve(__dirname, '../legacy-fec-codebase/frontend-capstone/dist/index.html'));
  });

  // getReviewsOfProduct route
  // TO-DO: incorporate sort string into query
  app.get('/reviews/:id', (req, res) => {
    var params = req.params.id;
    var query = [req.query.count, req.query.sort];
    reviewsCollection.find({ 'product_id': Number(params)}).toArray((err, data) => {
      if (err) {
        console.error('Error: cannot retreive product\'s reviews', err);
      } else {
        var queryData = {
          '_id': data[0]['_id'],
          'product_id': data[0]['product_id'],
          'characteristics': data[0]['characteristics'],
          'reviews': data[0]['reviews'].slice(0, query[0])
        };
        res.status(200).send(queryData);
      }
    });
  });

  // getReviewMetaData route
  app.get('/reviews/:id/meta', (req, res) => {
    var params = req.params.id;
  });

  // postNewReview route
  app.post('/reviews/:id/', (req, res) => {
    var id = {
      _id: new ObjectID(req.params.id)
    };
  });

});