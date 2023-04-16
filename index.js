const express = require('express');
const favicon = require('serve-favicon')
const path = require('path')
const app = express();
require('dotenv').config();

const db = require('./database.js');

// For simulating crashes w/o shutting down database instances.
const crash_config = {
	node1: false,
	node2: false,
	node3: false,
};

const HOST = process.env.HOST;
const PORT = process.env.PORT;

app.use(express.urlencoded({ extended: true }));
app.use('/public', express.static('public'));
app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
app.set('view engine', 'ejs');

app.get('/', express.json(), (req, res, next) => {
	req.crash_config = crash_config;
	db.getMovie(req, res, next);
});

app.post('/search', express.json(), (req, res, next) => {
	req.crash_config = crash_config;

	if (req.body.search_text !== '') {
		db.getMovieSearch(req, res, next);
	} 
});

app.get('/form', express.json(), (req, res, next) => {
	res.render('form', {});
});

app.get('/editform', express.json(), (req, res, next) => {
	res.render('editform', {});
});

app.post('/add', express.json(), (req, res, next) => {
	req.crash_config = crash_config;
	db.postAddMovie(req, res, next);;
});

app.post('/delete', express.json(), (req, res, next) => {
	res.send('delete');
	// db.postDeleteMovie(req, res, next);
});

app.post('/update', express.json(), (req, res, next) => {
	req.crash_config = crash_config;
	db.postUpdateMovie(req, res, next);
});

// Error handling middleware
app.use((err, req, res, next) => {
	console.log('Error Handler');
	console.log(err);

	if (err.status === 503) {
		res.status(503).render('error', {message: err.message});
	} else {
		res.send('Hotdog');
	}
});

app.listen(PORT, (err) => {
	console.log(`> Web App is online at http://${HOST}:${PORT}`);
});