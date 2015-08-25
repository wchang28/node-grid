var http = require('http');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var fs = require('fs');
var stompConnector = require('stomp_msg_connector');
var os = require("os");

var MAIN_HANDLER_PATH = "/grid_task_launcher";
var REQUIRED_SQL_KEYS = ["GetTaskDetail", "MarkTaskStart" ,"MarkTaskEnd"];

// argv[2] is config file
if (process.argv.length < 3) {
	console.error('config file is not optional');
	process.exit(1);
}
var gridConfigFilePath = process.argv[2];
console.log('grid config file path: ' + gridConfigFilePath);
var config = JSON.parse(fs.readFileSync(gridConfigFilePath, 'utf8'));

// determine the IPv4 address to use
//////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
if (addresses.length > 0) {
	var interfaceIndex = 0;
	if (addresses.length > 1) {	// multiple IPv4 address (multi-home/multiple network adaptor)
		console.log('deletected multiple IPv4 interfaces: ' + JSON.stringify(addresses));
		interfaceIndex = config['IPv4InterfaceIndex']
		if (typeof interfaceIndex !== 'number' || interfaceIndex < 0 || interfaceIndex >= addresses.length) {
			console.error('must specified a valid IPv4 interface index from range [0, ' + addresses.length +  ') in config file');
			process.exit(1);
		}
	}
	ipAddress = addresses[interfaceIndex];
	console.log('IPv4 address selected: ' + ipAddress);
} else {	// no IPv4 address
	console.error('no local IPv4 address detected');
	process.exit(1);
}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////

// configure the node object
//////////////////////////////////////////////////////////////////////////////////////////////////////////////
if (typeof config["dispatchPort"] != 'number' || config["dispatchPort"] <= 0) {
	console.error('dispatchPort not specified in config file');
	process.exit(1);
}
if (typeof config["availableCPUs"] != 'number' || config["availableCPUs"] <= 0) {
	console.error('availableCPUs not specified in config file');
	process.exit(1);
}
config["node"] =
{
	name: os.hostname()
	,ip: ipAddress
	,port: config["dispatchPort"]
	,use_ip: (typeof config["dispatchUseIP"] === 'boolean' ? config["dispatchUseIP"] : true);
	,num_cpus: config["availableCPUs"]
};
console.log("node: " + JSON.stringify(config["node"]));
delete config['dispatchPort'];
delete config['dispatchUseIP'];
delete config['availableCPUs'];
//////////////////////////////////////////////////////////////////////////////////////////////////////////////

// configure the message broker
//////////////////////////////////////////////////////////////////////////////////////////////////////////////
if (!config['msgBroker']) {
	console.error('msgBroker not specified in config');
	process.exit(1);
}
var dispatcherToTaskLauncherTopic = config["dispatcherToTaskLauncherTopic"];
if (typeof dispatcherToTaskLauncherTopic !== 'string' || dispatcherToTaskLauncherTopic.length == 0) {
	console.error('dispatcherToTaskLauncherTopic not specified in config file');
	process.exit(1);
}
config['brokers'] = {};
config['brokers']['mainMsgBroker'] = config['msgBroker'];
delete config['msgBroker'];
var mainBrokerConfig = config["brokers"]["mainMsgBroker"];
mainBrokerConfig["processors"] = {};
mainBrokerConfig["processors"]["topicProcessor"] =
{
	"incoming": dispatcherToTaskLauncherTopic
	,"handler_path": __dirname + MAIN_HANDLER_PATH
	,"handler_key": "dispatcherMsgHandler"
};
var taskLauncherToDispatcherQueue = config['taskLauncherToDispatcherQueue'];
if (typeof taskLauncherToDispatcherQueue !== 'string' || taskLauncherToDispatcherQueue.length == 0) {
	console.error('taskLauncherToDispatcherQueue not specified in config file');
	process.exit(1);
}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////

// check database settings
//////////////////////////////////////////////////////////////////////////////////////////////////////////////
var dbSettings = config['db_conn'];
if (!dbSettings) {
	console.error('db configuration not specified in config');
	process.exit(1);
}

if (typeof dbSettings.conn_str !== 'string' || dbSettings.conn_str.length == 0) {
	console.error('db connection string not specified in config');
	process.exit(1);
}

for (var i in REQUIRED_SQL_KEYS) {
	var sqlKey = REQUIRED_SQL_KEYS[i];
	if (!dbSettings.sqls || typeof dbSettings.sqls[sqlKey] !== 'string' || dbSettings.sqls[sqlKey].length == 0) {
		console.error('sql key ' + sqlKey + ' not specified in config');
		process.exit(1);
	}
}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////

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