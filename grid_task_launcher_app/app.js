var http = require('http');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var fs = require('fs');
var stompConnector = require('stomp_msg_connector');
var os = require("os");

var MAIN_HANDLER_PATH = "/grid_task_launcher";

var interfaces = os.networkInterfaces();
var addresses = [];
for (var k in interfaces) {
    for (var k2 in interfaces[k]) {
        var address = interfaces[k][k2];
        if (address.family === 'IPv4' && !address.internal) {
            addresses.push(address.address);
        }
    }
}
var ipAddress = null;
if (addresses.length > 0)
	ipAddress = addresses[0];
else {
	console.error('no local ip address');
	process.exit(1);
}

// argv[2] is config file
if (process.argv.length < 3) {
	console.error('config file is not optional');
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
mainBrokerConfig["processors"]["topicProcessor"] =
{
	"incoming": config["dispatcherToTaskLauncherTopic"]
	,"handler_path": __dirname + MAIN_HANDLER_PATH
	,"handler_key": "dispatcherMsgHandler"
};
// configure the node object
config["node"] =
{
	name: os.hostname()
	,ip: ipAddress
	,port: config["dispatchPort"]
	,use_ip: config["dispatchUseIP"]
	,num_cpus: config["availableCPUs"]
};
delete config['dispatchPort'];
delete config['dispatchUseIP'];
delete config['availableCPUs'];

//console.log("===============================================");
//console.log(JSON.stringify(config));
//console.log("===============================================");

app.use(bodyParser.json());
app.use(function timeLog(req, res, next) {
	console.log('an incomming request @ ./. Time: ', Date.now());
	next();
});

// initialize the connector
var p = stompConnector.initialize(config);
p.on('broker_connected', function (event) {
	console.log(event.broker_name + ': connected to the msg broker ' + event.broker_url);
}).on('ready', function () {
	console.log('messaging service is READY');
	// setup the main app route
	var gridTaskLauncherRoutes = require(__dirname + MAIN_HANDLER_PATH).router;
	app.use(MAIN_HANDLER_PATH, gridTaskLauncherRoutes);
	// launch the web server
	var server = http.createServer(app);
	server.listen(config.node.port, function() {
		var host = server.address().address;
		var port = server.address().port;
		console.log('task launcher server listening at %s://%s:%s', 'http', host, port);
		// try to join the grid
		var msgBroker = stompConnector.getBroker('mainMsgBroker');
		var o = {method: 'nodeRequestToJoinGrid', content: {node: config["node"]}};
		msgBroker.send(config['taskLauncherToDispatcherQueue'], {persistence: true}, JSON.stringify(o), function(recepit_id) {
			console.log('nodeRequestToJoinGrid message sent successfully. recepit_id=' + recepit_id);
		});
	});
});