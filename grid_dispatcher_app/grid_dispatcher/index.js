// grid dispatcher web services
// handler for route /grid_dispatcher
var xmldom = require('xmldom');
var DOMParser = xmldom.DOMParser;
var XMLSerializer = xmldom.XMLSerializer;
var xpath = require('xpath');
var fs = require('fs');
var Dispatcher = require('node-grid').GridDispatcher;
var express = require('express');
var router = express.Router();
var stompConnector = require('stomp_msg_connector');

var config = stompConnector.getConfig();
var msgBroker = stompConnector.getBroker('mainMsgBroker');
var dispatcher = new Dispatcher(msgBroker, config["dispatcherToTaskLauncherTopic"], config["taskDispatchQueue"], config.db_conn);
var eventTopic = config["eventTopic"];

// hookup the dispatcher events
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
dispatcher.onNodesStatusChanged = function (event) {
	msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_NODES_STATUS_CHANGED', content: event}),function(receipt_id) {});
};
dispatcher.onNodeAdded = function (newNode) {
	console.log("node " + JSON.stringify(newNode) + " just joint the grid");
	msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_NODE_ADDED', content: newNode}),function(receipt_id) {});
};
dispatcher.onNodeDisabled = function(node, leaveGrid) {
	msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_NODE_DISABLED', content: {node: node, leaveGrid: leaveGrid}}),function(receipt_id) {});
};
dispatcher.onNodeRemoved = function (nodeRemoved) {
	msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_NODE_REMOVED', content: nodeRemoved}),function(receipt_id) {});
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
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

function make_err_obj(err) {return {exception: err.toString()};}

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
	if (job.cookie) nodeJob.setAttribute('cookie', job.cookie);
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
	try {
		var job = request.body;
		var xml = makeJobXml(job);
		var submitToken = (job.submit_token ? job.submit_token : null);
		parseSubmitJobParams(xml
		,function(newJob, job_xml)	{
			if (job.description) newJob.description = job.description;
			if (job.cookie) newJob.cookie = job.cookie;
			dispatcher.submitJob(newJob, job_xml, submitToken, onFinalReturn, onFinalError);
		}, onFinalError);
	} catch(e) {
		onFinalError(e);
	}
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
	try {
		getJobIdFromRequest(request, function(err, job_id) {
			console.log('KILL: getting a kill job request job_id=' + job_id);
			if (err)
				onFinalError(err);
			else
				dispatcher.killJob(job_id, onFinalReturn, onFinalError);
		});
	} catch(e) {
		onFinalError(e);
	}
}

function handleGetJobProgress(request, result) {
	function onFinalError(err) {
		console.log('!!! Error: ' + err.toString());
		result.json(make_err_obj(err));
	}
	function onFinalReturn(data) {
		result.json(data);
	}
	try {
		getJobIdFromRequest(request, function(err, job_id) {
			if (err)
				onFinalError(err);
			else {
				dispatcher.getJobProgress(job_id, function(err, data) {
					if (err)
						onFinalError(err);
					else
						onFinalReturn(data);
				});
			}
		});
	} catch(e) {
		onFinalError(e);
	}
}

function handleGetJobResult(request, result) {
	function onFinalError(err) {
		console.log('!!! Error: ' + err.toString());
		result.json(make_err_obj(err));
	}
	function onFinalReturn(data) {
		result.json(data);
	}
	try {
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
	} catch(e) {
		onFinalError(e);
	}
}

function handleGetGridState(request, result) {
	function onFinalError(err) {
		console.log('!!! Error: ' + err.toString());
		result.json(make_err_obj(err));
	}
	function onFinalReturn(data) {
		result.json(data);
	}
	try {
		onFinalReturn(dispatcher.getGridState());
	} catch(e) {
		onFinalError(e);
	}
}

function getNodeHostFromRequest(request, onDone) {
	if (typeof onDone === 'function') {
		if (!request.query || !request.query.node || request.query.node.length == 0)
			onDone('bad node', null, request.query);
		else
			onDone(null, request.query.node, request.query);
	}
}

function handleEnableNode(request, result) {
	function onFinalError(err) {
		console.log('!!! Error: ' + err.toString());
		result.json(make_err_obj(err));
	}
	function onFinalReturn() {result.json({});}
	try {
		getNodeHostFromRequest(request, function(err, host, query) {
			if (err)
				onFinalError(err);
			else {
				dispatcher.enableNode(host);
				onFinalReturn();
			}
		});
	} catch(e) {
		onFinalError(e);
	}	
}

function handleDisableNode(request, result) {
	function onFinalError(err) {
		console.log('!!! Error: ' + err.toString());
		result.json(make_err_obj(err));
	}
	function onFinalReturn() {result.json({});}
	try {
		getNodeHostFromRequest(request, function(err, host, query) {
			if (err)
				onFinalError(err);
			else {
				var leaveGrid = false;
				if (query && query.leaveGrid && (query.leaveGrid === '' || query.leaveGrid === '1' || query.leaveGrid.toUpperCase() === 'TRUE')) leaveGrid = true;
				dispatcher.disableNode(host, leaveGrid);
				onFinalReturn();
			}
		});
	} catch(e) {
		onFinalError(e);
	}	
}

router.use(function timeLog(req, res, next) {
	console.log('an incomming request @ /grid_dispatcher. Time: ', Date.now()); 
	res.header("Cache-Control", "no-cache, no-store, must-revalidate");
	res.header("Pragma", "no-cache");
	res.header("Expires", 0);
 	next(); 
}); 
 
router.post('/submit_job', handleSubmitJob);
router.get('/kill_job', handleKillJob);
router.get('/get_job_progress', handleGetJobProgress); 
router.get('/get_job_result', handleGetJobResult);
router.get('/get_grid_state', handleGetGridState);
router.get('/enable_node', handleEnableNode);
router.get('/disable_node', handleDisableNode);

router.all('/', function(request, result) {
	result.set('Content-Type', 'application/json');
	result.json(make_err_obj('bad request'));
});

module.exports["router"] = router;
module.exports["taskLauncherMsgHandler"] = function(broker, message) {
	dispatcher.handleTaskLauncherMsg(JSON.parse(message.body));
};