const express = require('express');
const app = express();
const port = 8080;
const path = require('path');
const bodyParser = require('body-parser');
// maybe need to install and use cors?

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static('/legacy-fec-codebase/frontend-capstone/dist'));

app.get('/', (req, res) => {
  res.sendFile(path.join('/legacy-fec-codebase/frontend-capstone/dist/index.html'));
});

// Define other routes here

app.listen(port, () =>
  console.log(`Example app listening at http://localhost:${port}`)
);
