// grid dispatcher web services
var xmldom = require('xmldom');
var DOMParser = xmldom.DOMParser;
var XMLSerializer = xmldom.XMLSerializer;
var xpath = require('xpath');
var fs = require('fs');
var Stomp = require('stompjs2');
var StompMsgBroker = require('stomp_msg_broker').StompMsgBroker;
var Dispatcher = require('node-grid').GridDispatcher;
var express = require('express');
var router = express.Router();
	
var msgBroker = null;
var dispatcher = null;

function initialize(config) {
	var brokerConfig = config.msg_broker;
	msgBroker = new StompMsgBroker(function() {return Stomp.client(brokerConfig.url, null, brokerConfig.tlsOptions);}, brokerConfig.broker_options, brokerConfig.login_options, null);
	var eventTopic = brokerConfig.event_topic;
	dispatcher = new Dispatcher(config.task_monitor_port, config.launcher_url_home_path, config.db_conn, config.nodes);
	
	msgBroker.onconnect = function() {
		var s = 'connected to the msg broker ' + msgBroker.url;
		console.log(s);
	}
	dispatcher.onNodesStatusChanged = function (event) {
		msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_NODES_STATUS_CHANGED', content: event}),function(receipt_id) {});
	};
	dispatcher.onQueueChanged = function (event) {
		msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_QUEUE_CHANGED', content:event}),function(receipt_id) {});
	};
	dispatcher.onNewJobSubmitted = function (job_id, submitToken) {
		var content = {job_id: job_id};
		if (submitToken) content.submit_token = submitToken;
		msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_NEW_JOB_SUBMITTED', content:content}),function(receipt_id) {});
	};
	dispatcher.onJobStatusChanged = function(job_progress) {
		msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_JOB_STATUS_CHANGED', content:job_progress}),function(receipt_id) {});
	};
	dispatcher.onJobRemovedFromTracking = function (job_id) {
		var content = {job_id: job_id};
		msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_JOB_REMOVED_FROM_TRACKING', content:content}),function(receipt_id) {});
	};
	dispatcher.onTaskCompleted = function (task) {
		msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_TASK_COMPLETED', content:task}),function(receipt_id) {});
	};
}

function make_err_obj(err) { 
 	var o = {exception: err.toString()}; 
 	return o;
} 

function parseSubmitJobParams(body, onDone, onError) {
	try	{
		var doc = new DOMParser().parseFromString(body == '' ? '<?xml version="1.0" encoding="UTF-8"?>' : body, 'text/xml');
		var nodeJob = xpath.select1("/params/job", doc);
		var nlTasks = xpath.select("tasks/task", nodeJob);
		var num_tasks = nlTasks.length;
		var job = {tasks: []};
		for (var i = 0; i < num_tasks; i++)
			job.tasks.push({index: i});
		var job_xml = new XMLSerializer().serializeToString(nodeJob);
		if (typeof onDone == 'function') onDone(job, job_xml);
	} catch (e) {
		if (typeof onError == 'function') onError(e.toString());
	}
}

function makeJobXml(job) {
	var doc = new DOMParser().parseFromString('<?xml version="1.0" encoding="UTF-8"?>');
	var root = doc.createElement('params');
	doc.appendChild(root);
	var nodeJob = doc.createElement('job');
	root.appendChild(nodeJob);
	if (job.description) nodeJob.setAttribute('description', job.description);
	var nodeTasks = doc.createElement('tasks');
	nodeJob.appendChild(nodeTasks);
	for (var i in job.tasks) {
		var task = job.tasks[i];
		var nodeTask = doc.createElement('task');
		nodeTask.setAttribute('cmd', task.cmd);
		if (task.cookie) nodeTask.setAttribute('cookie', task.cookie);
		nodeTasks.appendChild(nodeTask);
	}
	var xml = new XMLSerializer().serializeToString(doc);
	return xml;
}

function handleSubmitJob(request, result) {
	function onFinalError(err) {
		console.log('!!! Error: ' + err.toString());
		result.json(make_err_obj(err));
	}
	function onFinalReturn(job_id) {
		result.json({job_id: job_id});
	}
	var job = request.body;
	var xml = makeJobXml(job);
	var submitToken = (job.submit_token ? job.submit_token : null);
	parseSubmitJobParams(xml
	,function(job, job_xml) {dispatcher.submitJob(job, job_xml, submitToken, onFinalReturn, onFinalError);}
	,onFinalError);
}

function getJobIdFromRequest(request, onDone) {
	if (!request.query || !request.query.job_id) {
		if (typeof onDone === 'function') onDone('bad job id', null);
		return;
	}
	var job_id = parseInt(request.query.job_id);
	if (isNaN(job_id) || job_id <= 0) {
		if (typeof onDone === 'function') onDone('bad job id', null);
	} else {
		if (typeof onDone === 'function') onDone(null, job_id);
	}
}

function handleKillJob(request, result) {
	function onFinalError(err) {
		console.log('!!! Error: ' + err.toString());
		result.json(make_err_obj(err));
	}
	function onFinalReturn() {
		result.json({});
	}
	getJobIdFromRequest(request, function(err, job_id) {
		if (err)
			onFinalError(err);
		else
			dispatcher.killJob(job_id, onFinalReturn, onFinalError);
	});
}

function handleGetJobResult(request, result) {
	function onFinalError(err) {
		console.log('!!! Error: ' + err.toString());
		result.json(make_err_obj(err));
	}
	function onFinalReturn(data) {
		result.json(data);
	}
	getJobIdFromRequest(request, function(err, job_id) {
		if (err)
			onFinalError(err);
		else {
			dispatcher.getJobResult(job_id, function(err, data) {
				if (err)
					onFinalError(err);
				else
					onFinalReturn(data);
			});
		}
	});
}

router.use(function timeLog(req, res, next) { 
	//console.log('an incomming request @ /grid_dispatcher. Time: ', Date.now()); 
 	next(); 
}); 
 
router.post('/submit_job', handleSubmitJob);
router.get('/kill_job', handleKillJob); 
router.get('/get_job_result', handleGetJobResult);

router.all('/', function(request, result) {
	result.set('Content-Type', 'application/json');
	result.json(make_err_obj('bad request'));
});

module.exports.initialize = initialize;
module.exports.router = router;