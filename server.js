const path = require('path');
const express = require('express');
const bodyParser = require('body-parser');
const MongoClient = require('mongodb').MongoClient;
const app = express();
const port = 8080;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
// app.use(express.static(path.resolve(__dirname, '../legacy-fec-codebase/frontend-capstone/dist/index.html')));

// manually run index.js to start ETL process -- but how???

MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true }, (err, client) => {
  if (err) {
    return console.log('Error: unable to connect to MongoDB');
  }
  app.listen(port, () => {
    console.log(`Express server listening at http://localhost:${port}`);
  });
  console.log(`Connected MongoDB: ${url}`);
  console.log(`Database: ${dbName}`);
  const db = client.db(dbName);
  const reviewsCollection = db.collection('reviews');

  app.get('/', (req, res) => {
    res.sendFile(path.resolve(__dirname, '../legacy-fec-codebase/frontend-capstone/dist/index.html'));
  });

  // getReviewsOfProduct route
  app.get('/reviews/:id/list?sort=:sortString:asc&count=:count}', (req, res) => {
    var params = [req.params.id, req.params.sortString, req.params.count];
    reviewsCollection.find({ product_id: params[0], reviews: {qty: params[2]}}).limit(5);
  });

  // getReviewMetaData route
  app.get('/reviews/:id/meta', (req, res) => {
    var params = req.params.id;
  });

});


