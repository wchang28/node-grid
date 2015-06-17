var http = require('http');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var fs = require('fs');

// process.argv[2] is config path
if (process.argv.length < 3) {
	console.error('config file not optional');
	process.exit(1);
}
var gridConfigFilePath = process.argv[2];
console.log('grid config file path: ' + gridConfigFilePath);
var config = JSON.parse(fs.readFileSync(gridConfigFilePath, 'utf8'));

// process.argv[3] is tcp port
var DEFAULT_PORT = 279;
var tcp_port = (process.argv.length >=4 ? (parseInt(process.argv[3]) ? parseInt(process.argv[3]) : DEFAULT_PORT) : DEFAULT_PORT);

app.use(bodyParser.json());
app.use(function timeLog(req, res, next) {
	//console.log('an incomming request @ ./. Time: ', Date.now());
	next();
});

var gridDispatcher = require('./grid_dispatcher/');
gridDispatcher.initialize(config);
app.use('/grid_dispatcher', gridDispatcher.router);

var server = http.createServer(app);

server.listen(tcp_port, function() {
	var host = server.address().address;
	var port = server.address().port;
	console.log('Grid dispatcher listening at %s://%s:%s', 'http', host, port);
});
