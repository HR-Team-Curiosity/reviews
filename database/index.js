const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const MongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017';

const dbName = 'sdc';

MongoClient.connect(url, { useNewUrlParser: true }, (err, client) => {
  if (err) {
    return console.log('Error: unable to connect to MongoDB');
  }
  console.log(`Connected MongoDB: ${url}`);
  console.log(`Database: ${dbName}`);
  const db = client.db(dbName);
  const reviewsCollection = db.collection('reviews');

  db.listCollections({ name: 'reviews' })
    .next((err, collectionInfo) => {
      if (collectionInfo) {
        reviewsCollection.drop();
      } else {
        db.createCollection('reviews');
      }
    });

  fs.readFile(path.resolve(__dirname, '../data/trunc_data/product-100.csv'), 'utf8', (err, data) => {
    if (err) {
      console.log('Error: cannot read product-100.csv', err);
    } else {
      console.log('data', data);
      // console.log('parsed data', parse(data));
      parse(data, {
        columns: false,
        trim: true
      }, (err, productRows) => {
        if (err) {
          console.log('Error: cannot parse data');
        } else {
          console.log('productRows', productRows);
        }
        // bulk.insert(productRow);
      });
    };
  });
  // bulk.execute();

  fs.readFile('/data/trunc_data/product-100.csv', (err, data) => {
    parse(data, { columns: false, trim: true }, (err, productRows) => {
      productRows.forEach(productRow => {
        var product = {
          'product_id': 0,
          'name': '',
          'recommend': {
            'true': 0,
            'false': 0
          },
          'characteristics': {},
          'ratings': {
            '1': 0,
            '2': 0,
            '3': 0,
            '4': 0,
            '5': 0
          },
          'reviews': []
        };
        product['product_id'] = Number(productRow[0]);
        product['name'] = productRow[1];

        // other file operations
        fs.readFile('/data/trunc_data/reviews-100.csv', (err, data) => {
          parse(data, { columns: false, trim: true }, (err, reviewRows) => {
            var reviewRecords = reviewRows.filter(reviewRow => Number(reviewRow[1]) === product['product_id']);
            reviewRecords.forEach(reviewRecord => {
              var review = {
                'id': 0,
                'rating': 0,
                'date': '',
                'summary': '',
                'body': '',
                'recommend': '',
                'reported': '',
                'reviewer_name': '',
                'reviewer_email': '',
                'response': '',
                'helpfulness': 0,
                'photos': []
              };
              if (reviewRecord[6] === 'true' || reviewRecord[6] === 'false') {
                if (reviewRecord[6] === 'true') {
                  review['recommend'] = 1;
                } else {
                  review['recommend'] = 0;
                }
              }
              if (reviewRecord[7] === 'true' || reviewRecord[7] === 'false') {
                if (reviewRecord[7] === 'true') {
                  review['reported'] = 1;
                } else {
                  review['reported'] = 0;
                }
              }
              if (reviewRecord[10] === '') {
                review['response'] = null;
              } else {
                review['response'] = reviewRecord[10];
              }
              review['id'] = Number(reviewRecord[0]);
              review['rating'] = Number(reviewRecord[2]);
              review['date'] = reviewRecord[3];
              review['summary'] = reviewRecord[4];
              review['body'] = reviewRecord[5];
              review['reviewer_name'] = reviewRecord[8];
              review['reviewer_email'] = reviewRecord[9];
              review['helpfulness'] = Number(reviewRecord[11]);

              fs.readFile('/data/trunc_data/reviews_photos-100.csv', (err, data) => {
                parse(data, { columns: false, trim: true }, (err, photoRows) => {

                  var photoRecords = photoRows.filter(photoRow => Number(photoRow[1]) === review['id']);
                  photoRecords.forEach(photoRecord => {
                    var photo = {
                      'id': 0,
                      'url': ''
                    };
                    photo['id'] = photoRecord[0];
                    photo['url'] = photoRecord[2];
                    review['photos'].push(photo);
                  });
                });
              });
              product['reviews'].push(review);
            });
          });
        });
        bulk.insert(product);
      });
    });
  });

  // after finishing operations, client.close()
});

