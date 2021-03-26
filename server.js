const cors = require('cors');
const path = require('path');
const express = require('express');
const bodyParser = require('body-parser');
const url = 'mongodb://localhost:27017';
const ObjectID = require('mongodb').ObjectID;
const MongoClient = require('mongodb').MongoClient;
let db, reviewsCollection, idManager;
const dbName = 'sdc';
const app = express();
const port = 8080;

app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static(path.resolve(__dirname, './legacy-fec-codebase/frontend-capstone/dist')));

app.get('/', (req, res) => {
  res.sendFile(path.resolve(__dirname, './legacy-fec-codebase/frontend-capstone/dist/index.html'));
});

// TO-DO: incorporate sort string into query
app.get('/reviews/:id/list', (req, res) => {
  var params = req.params.id;
  var query = [req.query.count, req.query.sort];
  reviewsCollection.find({ 'product_id': Number(params) }).toArray((err, data) => {
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

app.post('/reviews/:id', (req, res) => {
  let newReview;
  var params = req.params.id;
  idManager.findOneAndUpdate({}, { $inc: { 'review_id': 1 }})
    .catch(err => {
      console.error('Error: cannot find and increment last review id', err);
    })
    .then(response => {
      newReview = {
        '_id': new ObjectID(Number(req.params.id)),
        'review_id': response.value['review_id'],
        'rating': req.body.rating,
        'date': new Date().toJSON().slice(0, 10).replace(/-/g, '-'),
        'summary': req.body.summary,
        'body': req.body.body,
        'recommend': req.body.recommend,
        'reported': false,
        'reviewer_name': req.body.name,
        'reviewer_email': req.body.email,
        'response': null,
        'helpfulness': 0,
        'photos': req.body.photos,
        'characteristics': req.body.characteristics
      };
    })
    .then(response => {
      reviewsCollection.updateOne(
        { 'product_id': Number(params) },
        { $push: { 'reviews': newReview } },
        { 'hint': { 'product_id': 1 } },
        (err, data) => {
          if (err) {
            console.error(`Error: cannot create new review for product #${params}`, err);
          } else {
            res.status(201).send(data);
          }
        }
      );
    })
    .catch(err => {
      console.error(`Error: cannot create new review for product #${params}`, err);
    });
});

// TO-DO: finish ETL process for characteristics and build out meta reviews route
app.get('/reviews/:id/meta', (req, res) => {
  var params = req.params.id;
});

MongoClient.connect(url, { useNewUrlParser: true, useUnifiedTopology: true }, (err, client) => {
  if (err) {
    return console.log('Error: unable to connect to MongoDB');
  }
  console.log(`Connected MongoDB: ${url}`);
  console.log(`Database: ${dbName}`);
  db = client.db(dbName);
  reviewsCollection = db.collection('reviews');
  idManager = db.collection('idManager');
  app.listen(port, () => {
    console.log(`Express server listening at http://localhost:${port}`);
  });
});