var fs = require('fs');
var sqlserver = require('msnodesql');
var exec = require('child_process').exec;
var treeKill = require('tree-kill');
var stompConnector = require('stomp_msg_connector');

var msgBroker = stompConnector.getBroker('mainMsgBroker');
var config = stompConnector.getConfig();
var thisNode = config['node'];
//console.log(JSON.stringify(thisNode));
var MAX_NUM_TASKS_RUNNING_ALLOWED = thisNode.num_cpus;
var __dbSettings = null;			// database settings
var __leavePending = false;			// leave grig pending flag
var __acceptingNewTasks = false;	// accepting new tasks flag
var __numTasksRunning = 0;			// number of tasks that are currently running
var taskLauncherToDispatcherQueue = config['taskLauncherToDispatcherQueue'];

function task_toString(task) {return 'task{' + task.job_id + ',' + task.index + '}';}
function make_err_obj(err) {return {exception: err.toString()};}

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
function markTaskStartDB(conn_str, sql, task) {
	sqlserver.query(conn_str, sql, [task.job_id, task.index, task.pid], function(err, dataTable) {
		console.log(task_toString(task) + " start marked");
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

function ackTaskDispatchError(err, host) {
	var o = {method: 'nodeAckDispatchedTaskError', content: {exception: err.toString(), host: host}};
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

function markTaskStart(task) {markTaskStartDB(__dbSettings.conn_str, __dbSettings.sqls['MarkTaskStart'], task);}
function markTaskFinished(task, stdout, stderr, onDone) {markTaskFinishedDB(__dbSettings.conn_str, __dbSettings.sqls['MarkTaskEnd'], task, stdout, stderr, onDone);}

function runTask(task, onDone) {
	function onExit(err) {
		notifyDispatcherOnTaskFinished(task);	
		if (err)
			console.error('!!! Error: ' + err.toString());
		else
			console.log(task_toString(task) + " finished running");
		if (typeof onDone === 'function') onDone();
	}
	console.log(task.node + ' have received dispatch of ' + task_toString(task));
	getJobTaskInfoDB(__dbSettings.conn_str, __dbSettings.sqls['GetTaskDetail'], task, function(err) {
		if (err) {
			ackTaskDispatchError(err, task.node, onDone);
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

function notifyDispatcherNodeIsClearToLeaveGrid() {
	var o = {method: 'nodeIsClearToLeaveGrid', content:{host: thisNode.name}};
	msgBroker.send(taskLauncherToDispatcherQueue, {persistence: true}, JSON.stringify(o), function(recepit_id) {
		console.log('nodeIsClearToLeaveGrid message sent successfully. recepit_id=' + recepit_id);
	});	
}

function checkToLeaveGrid() {
	if (!__acceptingNewTasks && __leavePending && __numTasksRunning == 0) {
		console.log('node is now clear to leave the grid');
		notifyDispatcherNodeIsClearToLeaveGrid();
	}	
}

function onNumTasksRunningChanged() {
	console.log('numTasksRunning=' + __numTasksRunning);
	if (__numTasksRunning == 0) {
		console.log('node is idle');
		checkToLeaveGrid();
	}
}

function onNodeEnabled() {
	console.log("node enabled");
}

function onNodeDisabled() {
	console.log("node disabled, __leavePending=" + __leavePending);
	checkToLeaveGrid();
}

// task queue handler
module.exports['taskQueueMsgHandler'] = function(broker, message) {
	console.log('got message: ' + [__acceptingNewTasks, __numTasksRunning]);
	if (!__acceptingNewTasks || __numTasksRunning >= MAX_NUM_TASKS_RUNNING_ALLOWED) {
		message.nack();	// not accepting this message let other node handle it
	}
	else {	// __acceptingNewTasks && __numTasksRunning < MAX_NUM_TASKS_RUNNING_ALLOWED
		message.ack();
		var task = JSON.parse(message.body);
		task.node = thisNode.name;
		__numTasksRunning++;
		onNumTasksRunningChanged();
		runTask(task, function() {
			__numTasksRunning--;
			onNumTasksRunningChanged();
		});
	}
}

// topic handler
module.exports['dispatcherMsgHandler'] = function(broker, message) {
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
					console.error('!!! Error: ' + o.content.exception.toString());
					process.exit(1);
				}
				else {
					console.log(thisNode.name + " successfully join the grid");
					__dbSettings = content["dbSettings"];
					//console.log(JSON.stringify(__dbSettings));
					__acceptingNewTasks = true;
				}
				break;
			}
			case 'nodeAcceptTasks': {
				__acceptingNewTasks = content.accept;
				if (__acceptingNewTasks)
					__leavePending = false;	// clear the leave pending flag
				else	// not accepting new tasks
					__leavePending = content.leaveGrid;
				if (__acceptingNewTasks)
					onNodeEnabled();
				else
					onNodeDisabled();
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