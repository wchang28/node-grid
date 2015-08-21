var http = require('http');
var https = require('https');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var fs = require('fs');
var path = require('path');
var stompConnector = require('stomp_msg_connector');

var DISPATCHER_HOME_PATH = '/grid_dispatcher';
var MAIN_HANDLER_PATH = "." + DISPATCHER_HOME_PATH;

// process.argv[2] is config path
if (process.argv.length < 3) {
	console.error('config file not optional');
	process.exit(1);
}
var gridConfigFilePath = process.argv[2];
console.log('grid config file path: ' + gridConfigFilePath);
var config = JSON.parse(fs.readFileSync(gridConfigFilePath, 'utf8'));
config['brokers'] = {};
config['brokers']['mainMsgBroker'] = config['msgBroker'];
delete config['msgBroker'];
var mainBrokerConfig = config["brokers"]["mainMsgBroker"];
mainBrokerConfig["processors"] = {};
mainBrokerConfig["processors"]["mainProcessor"] =
{
	"incoming": config["taskLauncherToDispatcherQueue"]
	,"subscribe_headers": {"ack": "client"}
	,"handler_path": __dirname + DISPATCHER_HOME_PATH
	,"handler_key": "taskLauncherMsgHandler"
};

//console.log("===============================================");
//console.log(JSON.stringify(config));
//console.log("===============================================");
	
var p = stompConnector.initialize(config);
p.on('broker_connected', function (event) { 
 	console.log(event.broker_name + ': connected to the msg broker ' + event.broker_url); 
}).on('ready', function () { 
	console.log('messaging service is READY'); 
}); 
	
// process.argv[3] is tcp port
var DEFAULT_PORT = 279;
var tcp_port = (process.argv.length >=4 ? (parseInt(process.argv[3]) ? parseInt(process.argv[3]) : DEFAULT_PORT) : DEFAULT_PORT);

app.use(bodyParser.json());
app.use(function timeLog(req, res, next) {
	//console.log('an incomming request @ ./. Time: ', Date.now());
	res.header("Access-Control-Allow-Origin", "*");
	next();
});

app.use(DISPATCHER_HOME_PATH, require(MAIN_HANDLER_PATH).router);

var server = null;
var secure_http = false;
var sslCredentials = null;
if (config.ssl) {
	secure_http = true;
	var privateKey  = fs.readFileSync(config.ssl.private_key_file, 'utf8');
	var certificate = fs.readFileSync(config.ssl.certificate_file, 'utf8');
	sslCredentials = {key: privateKey, cert: certificate};
	if (config.ssl.ca_files && config.ssl.ca_files.length > 0) {
		var ca = [];
		for (var i in config.ssl.ca_files)
			ca.push(fs.readFileSync(config.ssl.ca_files[i], 'utf8'));
		sslCredentials.ca = ca;
	}
	server = https.createServer(sslCredentials, app);
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

// console web services/web server support
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// process.argv[4] is console port
var DEFAULT_CONSOLE_PORT = 8080;
var console_port = (process.argv.length >=5 ? (parseInt(process.argv[4]) ? parseInt(process.argv[4]) : DEFAULT_CONSOLE_PORT) : DEFAULT_CONSOLE_PORT);

// global is the configuration for the console browser client
////////////////////////////////////////////////////////////////////////////////
var global = require('./global');
global.msgBrokerConfig =
{
	"url": config["consoleBrowserClient"]["brokerUrl"]
	,"brokerOptions": mainBrokerConfig["brokerOptions"]
	,"loginOptions": mainBrokerConfig["loginOptions"]
	,"tlsOptions": (mainBrokerConfig["tlsOptions"] ? mainBrokerConfig["tlsOptions"] : null)
	,"eventTopic": config["eventTopic"]
};
global.dispatcher = {};
global.dispatcher.port = tcp_port;
global.dispatcher.rootPath = DISPATCHER_HOME_PATH;
global.dispatcher.protocol = (secure_http ? 'https://' : 'http://');
////////////////////////////////////////////////////////////////////////////////

var appConsole = express();
appConsole.use(bodyParser.json());
appConsole.use(function timeLog(req, res, next) {
	//console.log('an incomming request @ ./. Time: ', Date.now());
	res.header("Access-Control-Allow-Origin", "*");
	next();
});
appConsole.use('/grid/console', express.static(path.join(__dirname, 'console')));
appConsole.use('/grid/console_ws', require('./console_ws/').router);

var serverConsole = (secure_http ? https.createServer(sslCredentials, appConsole) : http.createServer(appConsole));
serverConsole.listen(console_port, function() {
	var host = serverConsole.address().address;
	var port = serverConsole.address().port;
	console.log('Grid console listening at %s://%s:%s', (secure_http ? 'https' : 'http'), host, port);
});
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////