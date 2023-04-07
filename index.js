const express = require('express');
const favicon = require('serve-favicon')
const path = require('path')
const app = express();
require('dotenv').config();

const db = require('./database.js');

// For simulating crashes w/o shuttomg down database instances.
const crash_config = {
	node1: false,
	node2: false,
	node3: false,
};

const HOST = process.env.HOST;
const PORT = process.env.PORT;

app.use('/public', express.static('public'));
app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
app.set('view engine', 'ejs');

app.get('/', express.json(), (req, res, next) => {
	req.crash_config = crash_config;
	db.getMovie(req, res, next);
});

app.get('/:search', express.json(), (req, res, next) => {
	res.send('hotdog');
});

app.post('/update', express.json(), (req, res, next) => {
	// update info
	res.send('hotdog');
});

app.post('/delete', express.json(), (req, res, next) => {
	// delete info
	res.send('hotdog');
});

app.post('/add', express.json(), (req, res, next) => {
	// add info
	res.send('hotdog');
});


app.use(function(err, req, res, next) {
	res.status(err.status || 500);
	res.send(err.message);
});

app.listen(PORT, (err) => {
	console.log(`> Web App is online at http://${HOST}:${PORT}`);
});