var fs = require('fs');
var sqlserver = require('msnodesql');
var exec = require('child_process').exec;
var treeKill = require('tree-kill');
var stompConnector = require('stomp_msg_connector');
var http = require('http');
var express = require('express');
var router = express.Router();

var msgBroker = stompConnector.getBroker('mainMsgBroker');
var config = stompConnector.getConfig();
var thisNode = config['node'];
//console.log(JSON.stringify(thisNode));
var __dbSettings = config['db_conn'];	// database settings
var __numTasksRunning = 0;				// number of tasks that are currently running
var __exitAfterLeaveGrid = (typeof config['exitAfterLeaveGrid'] === 'boolean' ? config['exitAfterLeaveGrid'] : false);
var taskLauncherToDispatcherQueue = config['taskLauncherToDispatcherQueue'];
var eventTopic = config['eventTopic'];

if (!Date.prototype.formatXMLDataTime) {
	Date.prototype.formatXMLDataTime = function () {
		function pad0_2(i) { return (i < 10 ? '0' + i : i.toString()); }
		function mil(ms) {
			var s = '';
			if (ms > 0) {
				s += '.';
				s += (ms < 10 ? '00' : ms < 100 ? '0' : '') + ms.toString();
				if (ms % 100 == 0)
					s = s.substr(0, s.length - 2);
				else if (ms % 10 == 0)
					s = s.substr(0, s.length - 1);
			}
			return s;
		}
		var m = this.getUTCMonth() + 1;
		return '' + this.getUTCFullYear() + '-' + pad0_2(m) + '-' + pad0_2(this.getUTCDate()) + 'T' + pad0_2(this.getUTCHours()) + ':' + pad0_2(this.getUTCMinutes()) + ':' + pad0_2(this.getUTCSeconds()) + mil(this.getUTCMilliseconds()) + 'Z';
	};
}
	
function task_toString(task) {return 'task{' + task.job_id + ',' + task.index + '}';}
function make_err_obj(err) {return {exception: err.toString()};}

function taskLauncherOnError(task, err) {
	var msg;
	if (task)
		msg = thisNode.name + ': task launcher error for ' + task_toString(task) + ": " + err.toString();
	else
		msg = thisNode.name + ': task launcher error: ' + err.toString();
	console.error('!!! ' + msg);
	
	var content = {
		"source": thisNode.name
		,"time": new Date().formatXMLDataTime()
		,"msg": msg
	};
	msgBroker.send(eventTopic, {persistent: true}, JSON.stringify({method: 'ON_ERROR', content:content}),function(receipt_id) {});
}

// Get the task info from the database so it can be launched.
// At the same time, mark the task as being 'DISPATCHED' to the node.
// If the task is not found or the task has already been aborted, the onError() handler will be called instead.
// onDone(err)
function getJobTaskInfoDB(conn_str, sql, task, onDone) {
	sqlserver.query(conn_str, sql, [task.job_id, task.index, task.node], function(err, dataTable) {
		if (err) {
			if (typeof onDone === 'function') onDone(err);
		} else {
			if (dataTable.length < 1) {
				if (typeof onDone === 'function') onDone(task_toString(task) + ' cannnot be found');
				return;
			} else {
				var row = dataTable[0];
				task.cmd = row['cmd'];
				task.cookie = row['cookie'];
				task.stdin_file = row['stdin_file'];
				task.aborted = row['aborted'];
				if (task.aborted) {
					if (typeof onDone === 'function') onDone(task_toString(task) + ' was aborted');
					return;
				} else {
					if (typeof onDone === 'function') onDone(null);
				}
			}
		}
	});
}

// mark task as 'RUNNING' in the database
function markTaskStartDB(conn_str, sql, task, onDone) {
	sqlserver.query(conn_str, sql, [task.job_id, task.index, task.pid], function(err, dataTable) {
		if (typeof onDone === 'function') onDone(err);
	});
}

// mark task as 'FINISHED' in the database
// onDone(err)
function markTaskFinishedDB(conn_str, sql, task, stdout, stderr, onDone) {
	var params =
	[
		task.job_id
		,task.index
		,task.pid
		,task.ret_code
		,stdout.length > 0 ? stdout : null
		,stderr.length > 0 ? stderr : null
	];
	sqlserver.query(conn_str, sql, params, function(err, dataTable) {
		if (typeof onDone === 'function') onDone(err);
	});
}

function ackTaskDispatchError(task, err) {
	var o = {method: 'nodeAckDispatchedTaskError', content: {task: task, exception: err.toString()}};
	msgBroker.send(taskLauncherToDispatcherQueue, {persistence: true}, JSON.stringify(o), function(recepit_id) {
		//console.log('nodeAckDispatchedTaskError message sent successfully for host ' + host + '. recepit_id=' + recepit_id);
	});
}
function ackTaskDispatch(task) {
	var o = {method: 'nodeAckDispatchedTask', content: {task: task}};
	msgBroker.send(taskLauncherToDispatcherQueue, {persistence: true}, JSON.stringify(o), function(recepit_id) {
		//console.log('nodeAckDispatchedTask message sent successfully for ' + task_toString(task) + '. recepit_id=' + recepit_id);
	});
}

// function to notify the dispatcher (via http) that the task has finished so the dispatcher can decrement the CPU usage count on the node
function notifyDispatcherOnTaskFinished(task) {
	var o = {method: 'nodeCompleteTask', content: {task: task}};
	msgBroker.send(taskLauncherToDispatcherQueue, {persistence: true}, JSON.stringify(o), function(recepit_id) {
		//console.log('nodeCompleteTask message sent successfully for ' + task_toString(task) + '. recepit_id=' + recepit_id);
	});
}

function markTaskStart(task) {
	markTaskStartDB(__dbSettings.conn_str, __dbSettings.sqls['MarkTaskStart'], task, function(err) {
		if (!err)
			console.log(task_toString(task) + " start marked");
		else 
			taskLauncherOnError(task, err);
	});
}
function markTaskFinished(task, stdout, stderr, onDone) {markTaskFinishedDB(__dbSettings.conn_str, __dbSettings.sqls['MarkTaskEnd'], task, stdout, stderr, onDone);}

function runTask(task, onDone) {
	function onExit(err) {
		notifyDispatcherOnTaskFinished(task);	
		if (err)
			taskLauncherOnError(task, err);
		else
			console.log(task_toString(task) + " finished running");
		if (typeof onDone === 'function') onDone();
	}
	console.log(task.node + ' have received dispatch of ' + task_toString(task));
	getJobTaskInfoDB(__dbSettings.conn_str, __dbSettings.sqls['GetTaskDetail'], task, function(err) {
		if (err) {
			ackTaskDispatchError(task, err);
			if (typeof onDone === 'function') onDone();
			return;
		} else {
			var cmd = task.cmd;
			var stdin_file = task.stdin_file;
			var cookie = task.cookie;
			delete task["cmd"];
			delete task['stdin_file'];
			delete task['cookie'];
			task.pid = 0;
			var stdout = '';
			var stderr = '';
			var instream = null;
			if (stdin_file && typeof stdin_file == 'string' && stdin_file.length > 0) {
				instream = fs.openReadFileStream(stdin_file);
				if (!instream) {
					task.pid = 0;
					task.ret_code = 1;
					stderr = 'error opening stdin file ' + stdin_file;
					ackTaskDispatch(task);
					markTaskStart(task);
					markTaskFinished(task, stdout, stderr, onExit);
					return;
				}
			}
			var options = {};
			//console.log('cmd=' + cmd);
			var child = exec(cmd, options);
			if (instream && child.stdin) instream.pipe(child.stdin);
			task.pid = child.pid
			ackTaskDispatch(task);
			markTaskStart(task);
			child.stdout.on('data', function(data){
				stdout += data.toString();
			});
			child.stderr.on('data', function(data){
				stderr += data.toString();
			});
			child.on('error', function(err) {
				task.ret_code = err.code;
				console.log('child.on_error: ret=' + task.ret_code);
				markTaskFinished(task, stdout, stderr, onExit);
			});
			child.on('close', function(exitCode) {
				task.ret_code = exitCode;
				console.log('child.on_close: ret=' + task.ret_code);
				markTaskFinished(task, stdout, stderr, onExit);
			});				
		}
	});
}

function treeKillProcesses(pids) {
	for (var i in pids)
		treeKill(pids[i], 'SIGKILL');
}

function replyPing() {
	var o = {method: 'nodePing', content:{host: thisNode.name}};
	msgBroker.send(taskLauncherToDispatcherQueue, {persistence: true}, JSON.stringify(o), function(recepit_id) {
		console.log('nodePing reply sent successfully. recepit_id=' + recepit_id);
	});	
}

function onNumTasksRunningChanged() {
	console.log('numTasksRunning=' + __numTasksRunning);
	if (__numTasksRunning == 0)	console.log('node is idle');
}

function handleDispatchedTasks(request, result) {
	var tasks = request.body;
	result.json({});
	console.log('node received a dispatch of ' + tasks.length + ' task(s)');
	console.log(JSON.stringify(tasks));
	__numTasksRunning += tasks.length;
	onNumTasksRunningChanged();
	for (var i in tasks) {
		runTask(tasks[i], function() {
			__numTasksRunning--;
			onNumTasksRunningChanged();
		});
	}
}

router.use(function timeLog(req, res, next) {
	//console.log('an incomming request @ /grid_task_launcher. Time: ', Date.now());
	res.header("Cache-Control", "no-cache, no-store, must-revalidate");
	res.header("Pragma", "no-cache");
	res.header("Expires", 0);
	next();
});

router.post('/dispatched_tasks', handleDispatchedTasks);

router.all('/', function(request, result) {
	result.json(make_err_obj('bad request'));
});

module.exports.router = router;

// topic handler
module.exports.dispatcherMsgHandler = function(broker, message) {
	var o = JSON.parse(message.body);
	if (!o.host) {	// host not specify => broadcast
		switch(o.method) {
			case "nodePing": {
				replyPing();
				break;
			}
		}
	} else if (o.host === thisNode.name) {
		var content = o.content;
		switch(o.method) {
			case "nodeRequestToJoinGrid": {
				if (content.exception) {
					taskLauncherOnError(null, o.content.exception);
					process.exit(1);
				}
				else
					console.log('JOIN: ' + thisNode.name + " successfully join the grid");
				break;
			}
			case "nodeGridLeave": {
				console.log('LEAVE: ' + thisNode.name + " successfully leave the grid");
				if (__exitAfterLeaveGrid) {
					console.log('program exiting...');
					process.exit(0);
				}
				break;
			}
			case "nodeKillProcesses": {
				treeKillProcesses(content.pids);
				break;
			}
			case "nodePing": {
				replyPing();
				break;
			}
		}
	}
}

onNumTasksRunningChanged();