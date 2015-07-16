var http = require('http');
var https = require('https');
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

var server;
var secure_http = false;
if (config.ssl) {
	secure_http = true;
	var privateKey  = fs.readFileSync(config.ssl.private_key_file, 'utf8');
	var certificate = fs.readFileSync(config.ssl.certificate_file, 'utf8');
	var credentials = {key: privateKey, cert: certificate};
	server = https.createServer(credentials, app);
}
else {
	secure_http = false;
	server = http.createServer(app);
}

server.listen(tcp_port, function() {
	var host = server.address().address;
	var port = server.address().port;
	console.log('Grid dispatcher listening at %s://%s:%s', (secure_http ? 'https' : 'http'), host, port);
});

// process.argv[4] is tcp port
var DEFAULT_CONSOLE_PORT = 8080;
var console_port = (process.argv.length >=5 ? (parseInt(process.argv[4]) ? parseInt(process.argv[4]) : DEFAULT_CONSOLE_PORT) : DEFAULT_CONSOLE_PORT);

var appConsole = express();
appConsole.use('/console', path.join(__dirname, 'console'));

var serverConsole = http.createServer(appConsole);
serverConsole.listen(console_port, function() {
	var host = serverConsole.address().address;
	var port = serverConsole.address().port;
	console.log('Grid console listening at %s://%s:%s', 'http', host, port);
});