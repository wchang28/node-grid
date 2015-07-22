var GridClient = require('node-grid').GridClient;
var fs = require('fs');

// process.argv[2] is config path
if (process.argv.length < 3) {
	console.error('config file not optional');
	process.exit(1);
}
var configFilePath = process.argv[2];
console.log('config file path: ' + configFilePath);
var config = JSON.parse(fs.readFileSync(configFilePath, 'utf8'));
var gridClient = new GridClient(config['msg_broker'], config['gridEventTopic'], config['dispatcherConfig']);

var job = {
	"description": "testing"
	,"cookie": "12345"
	,"tasks":[]
};
var num_tasks = 100;
var cmd = "echo Hi from Wen Chang";
for (var i = 0; i < num_tasks; i++) {
	job.tasks.push({"cmd": cmd, "cookie": (i+1).toString()});
}

var p = gridClient.runJob(job);
p.on('connected', function() {
	console.log("client connected");
}).on('disconnected', function() {
	console.log("client disconnected");
}).on('error', function(err) {
	console.error(err.toString());
	process.exit(1);
}).on('job_submmitted', function(job_id) {
	console.log('Job Id = ' + job_id);
}).on('job_status_changed', function(jobProgress) {
	console.log('JP: ' + JSON.stringify(jobProgress));
}).on('task_completed', function(task) {
	console.log('TC: ' + JSON.stringify(task));
}).on('job_finished', function(status) {
	console.log('job ' + p.job_id + ' is ' + status);
}).on('success', function(dataTable) {
	var result = [];
	for (var i in dataTable) {
		var row = dataTable[i];
		var stdout = row['stdout'];
		result.push(stdout);
	}
	console.log('');
	console.log('Result');
	console.log('===========================================');
	console.log(JSON.stringify(result));
	console.log('===========================================');
	process.exit(0);
})