var http = require('http');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var fs = require('fs');

// process.argv[2] is tcp port
var DEFAULT_PORT = 277;
var tcp_port = (process.argv.length >=3 ? (parseInt(process.argv[2]) ? parseInt(process.argv[2]) : DEFAULT_PORT) : DEFAULT_PORT);

app.use(bodyParser.json());
app.use(function timeLog(req, res, next) {
	//console.log('an incomming request @ ./. Time: ', Date.now());
	next();
});

var gridTaskLauncherRoutes = require('./grid_task_launcher/');
app.use('/grid_task_launcher', gridTaskLauncherRoutes);

var server = http.createServer(app);

server.listen(tcp_port, function() {
	var host = server.address().address;
	var port = server.address().port;
	console.log('App server listening at %s://%s:%s', 'http', host, port);
});
