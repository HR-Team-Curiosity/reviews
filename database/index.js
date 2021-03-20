const MongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017';

const dbName = 'sdc';

MongoClient.connect(url, (err, client) => {
  if (err) {
    return console.log('Error: unable to connect to MongoDB');
  }
  const db = client.db(dbName);
  console.log(`Connected MongoDB: ${url}`);
  console.log(`Database: ${dbName}`);

  // client.close()
});

