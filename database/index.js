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


  // use csv-parser
  // dont need product id or name
  // fs.createReadStream (for each csv file!!) => repeat this step
  // then pipe into parser
  // inside for await loop:
  // clean data
  // group by productid
  // update 10k records at once (look at Mongo upsert command but slower than insert) => bulkWrite? (create variable to store # of records you've gotten, meet limit then execute, then clear variables)

  db.listCollections({ name: 'reviews' })
    .next((err, collectionInfo) => {
      if (collectionInfo) {
        reviewsCollection.drop();
      } else {
        db.createCollection('reviews');
      }
    });

  (async () => {

    const parser = fs.createReadStream(path.resolve(__dirname, '../data/trunc_data/reviews-100.csv'), 'utf8').pipe(csv())
    const mongoBulkWrite = () => {
      reviewsCollection.bulkWrite(operations, { ordered: true });
      operations = [];
      count = 0;
    };
    for await (const record of parser) {
      if (count >= 10000) {
        mongoBulkWrite();
      } else {
        if (record[6] === 'true' || record[6] === 'false') {
          if (record[6] === 'true') {
            var recommend = 1;
          } else {
            var recommend = 0;
          }
        }
        if (record[7] === 'true' || record[7] === 'false') {
          if (record[7] === 'true') {
            var reported = 1;
          } else {
            var reported = 0;
          }
        }
        if (record[10] === '') {
          var response = null;
        }
        // var document = { insertOne: {
        //   'id': Number(record[0]),
        //   'product_id': Number(record[1]),
        //   'rating': Number(record[2]),
        //   'date': record[3],
        //   'summary': record[4],
        //   'body': record[5],
        //   'recommend': recommend || Number(record[6]),
        //   'reported': reported || Number(record[7]),
        //   'reviewer_name': record[8],
        //   'reviewer_email': record[9],
        //   'response': response || record[10],
        //   'helpfulness': Number(record[11])
        // }}
        var document = {insertOne: {"hello": "world"}};
        operations.push(document);
        count++;
      }
    }
    mongoBulkWrite();
  })()
  .then(response => {
    parser.close();
  })
  .catch(err => {
    console.log('Error: cannot write to MongoDB')
  })

  // OLD STRATEGY: keeping this code here until I find a working strategy
  // fs.readFile(path.resolve(__dirname, '../data/trunc_data/product-100.csv'), 'utf8', (err, data) => {
  //   if (err) {
  //     console.log('Error: cannot read product-100.csv', err);
  //   } else {
  //     console.log('data', data);
  //     // console.log('parsed data', parse(data));
  //     parse(data, {
  //       columns: false,
  //       trim: true
  //     }, (err, productRows) => {
  //       if (err) {
  //         console.log('Error: cannot parse data');
  //       } else {
  //         console.log('productRows', productRows);
  //       }
  //       // bulk.insert(productRow);
  //     });
  //   };
  // });
  // // bulk.execute();

  // fs.readFile('/data/trunc_data/product-100.csv', (err, data) => {
  //   parse(data, { columns: false, trim: true }, (err, productRows) => {
  //     productRows.forEach(productRow => {
  //       var product = {
  //         'product_id': 0,
  //         'name': '',
  //         'recommend': {
  //           'true': 0,
  //           'false': 0
  //         },
  //         'characteristics': {},
  //         'ratings': {
  //           '1': 0,
  //           '2': 0,
  //           '3': 0,
  //           '4': 0,
  //           '5': 0
  //         },
  //         'reviews': []
  //       };
  //       product['product_id'] = Number(productRow[0]);
  //       product['name'] = productRow[1];

  //       // other file operations
  //       fs.readFile('/data/trunc_data/reviews-100.csv', (err, data) => {
  //         parse(data, { columns: false, trim: true }, (err, reviewRows) => {
  //           var reviewRecords = reviewRows.filter(reviewRow => Number(reviewRow[1]) === product['product_id']);
  //           reviewRecords.forEach(reviewRecord => {
  //             var review = {
  //               'id': 0,
  //               'rating': 0,
  //               'date': '',
  //               'summary': '',
  //               'body': '',
  //               'recommend': '',
  //               'reported': '',
  //               'reviewer_name': '',
  //               'reviewer_email': '',
  //               'response': '',
  //               'helpfulness': 0,
  //               'photos': []
  //             };
  //             if (reviewRecord[6] === 'true' || reviewRecord[6] === 'false') {
  //               if (reviewRecord[6] === 'true') {
  //                 review['recommend'] = 1;
  //               } else {
  //                 review['recommend'] = 0;
  //               }
  //             }
  //             if (reviewRecord[7] === 'true' || reviewRecord[7] === 'false') {
  //               if (reviewRecord[7] === 'true') {
  //                 review['reported'] = 1;
  //               } else {
  //                 review['reported'] = 0;
  //               }
  //             }
  //             if (reviewRecord[10] === '') {
  //               review['response'] = null;
  //             } else {
  //               review['response'] = reviewRecord[10];
  //             }
  //             review['id'] = Number(reviewRecord[0]);
  //             review['rating'] = Number(reviewRecord[2]);
  //             review['date'] = reviewRecord[3];
  //             review['summary'] = reviewRecord[4];
  //             review['body'] = reviewRecord[5];
  //             review['reviewer_name'] = reviewRecord[8];
  //             review['reviewer_email'] = reviewRecord[9];
  //             review['helpfulness'] = Number(reviewRecord[11]);

  //             fs.readFile('/data/trunc_data/reviews_photos-100.csv', (err, data) => {
  //               parse(data, { columns: false, trim: true }, (err, photoRows) => {

  //                 var photoRecords = photoRows.filter(photoRow => Number(photoRow[1]) === review['id']);
  //                 photoRecords.forEach(photoRecord => {
  //                   var photo = {
  //                     'id': 0,
  //                     'url': ''
  //                   };
  //                   photo['id'] = photoRecord[0];
  //                   photo['url'] = photoRecord[2];
  //                   review['photos'].push(photo);
  //                 });
  //               });
  //             });
  //             product['reviews'].push(review);
  //           });
  //         });
  //       });
  //       bulk.insert(product);
  //     });
  //   });
  // });

  // after finishing operations, client.close()
});

