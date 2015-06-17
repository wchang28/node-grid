// grid task launcher
var sqlserver = require('msnodesql');
var http = require('http');
var fs = require('fs');
var express = require('express');
var router = express.Router();

function task_toString(task) {return 'task{' + task.job_id + ',' + task.index + '}';}
function make_err_obj(err)
{
	var o = {excpetion: err.toString()};
	return o;
}

// Get the task info from the database so it can be launched.
// At the same time, mark the task as being 'DISPATCHED' to the node.
// If the task is not found or the task has already been aborted, the onError() handler will be called instead.
function getJobTaskInfoDB(conn_str, sql, task, onDone, onError) {
	var stmt = sqlserver.query(conn_str, sql, [task.job_id, task.index, task.node]);
	var ci = null;
	task.cmd = null;
	stmt.on('meta', function (meta)	{ci = meta;});
	stmt.on('column', function (idx, data, more) {
		var fld = ci[idx].name;
		switch(fld)
		{
			case 'cmd':
				task.cmd = data;
				break;
			case 'cookie':
				task.cookie = data;
				break;
			case 'stdin_file':
				task.stdin_file = data;
				break;
			case 'aborted':
				task.aborted = data;
				break;
		}
	});
	stmt.on('done', function ()	{
		if (!task.cmd) {
			if (typeof onError == 'function') onError(task_toString(task) + ' cannnot be found');
		}
		else if (task.aborted) {
			if (typeof onError == 'function') onError(task_toString(task) + ' was aborted');
		}
		else {
			if (typeof onDone == 'function') onDone();
		}
	});
	stmt.on('error', function (err) {
		if (typeof onError == 'function') onError(err.toString());
	})
}

// mark task as 'RUNNING' in the database
function markTaskStartDB(conn_str, sql, task) {
	var stmt = sqlserver.query(conn_str, sql, [task.job_id, task.index, task.pid]);
	stmt.on('done', function ()	{
		console.log(task_toString(task) + " start marked");
	});
}

// mark task as 'FINISHED' in the database
function markTaskFinishedDB(conn_str, sql, task, stdout, stderr, onDone, onError) {
	var params =
	[
		task.job_id
		,task.index
		,task.pid
		,task.ret_code
		,stdout.length > 0 ? stdout : null
		,stderr.length > 0 ? stderr : null
	];
	var stmt = sqlserver.query(conn_str, sql, params);
	var ci = null;
	stmt.on('meta', function (meta)	{ci = meta;});
	stmt.on('done', function ()	{
		console.log(task_toString(task) + " finished marked");
		if (typeof onDone == 'function') onDone(task, onError);
	});
	stmt.on('error', function (err) {
		if (typeof onError == 'function') onError(err.toString());
	})
}

function handleLaunchTask(request, result) {
	var dispatcher_address = request.connection.remoteAddress;
	//console.log('dispatcher address is ' + dispatcher_address);
	function onFinalError(err) {
		console.log('!!! ' + err.toString());
		result.json(make_err_obj(err));
	}
	function onFinalReturn(task) {
		console.log('child process pid=' + task.pid);
		result.json(task);
	}
	//console.log('request.body = ' + JSON.stringify(request.body));
	var lp = request.body;

	// function to notify the dispatcher (via http) that the task has finished so the dispatcher can decrement the CPU usage count on the node
	function notifyDispatcherOnTaskFinished(task, onError) {
		var options = {
			hostname: dispatcher_address
			,port: lp.otf.port
			,path: lp.otf.path
			,method: lp.otf.method
			,headers: {
				'Content-Type': 'application/json'
			}
		};
		var s = JSON.stringify(task);
		options.headers['Content-Length'] = s.length;
		//console.log(options);
		var req = http.request(options
		,function(res) {
			if (res.statusCode != 200) {
				if (typeof onError == 'function') onError('dispatcher returns a HTTP status code of ' + res.statusCode);
			}
			res.on('data', function(d) {
				try {
					var o = JSON.parse(d.toString());
					if (typeof o.exception == 'string') throw o.exception;
					console.log('dispatcher ACK task finished');
				} catch(e) {
					if (typeof onError == 'function') onError('dispatcher returns invalid response: ' + e);
				}
			});
		});
		req.on('error'
		,function(err) {
			if (typeof onError == 'function') onError('dispatcher response error - ' + err.message);
		});
		req.end(s);
	}
	//console.log(lp);
	var conn_str = lp.db.conn_str;
	var task = lp.task;
	console.log(task.node + ' have received dispatch of ' + task_toString(task));
	// get task info from the database
	getJobTaskInfoDB(conn_str, lp.db.sqls['GetTaskDetail'], task
	,function() {
		var instream = null;
		if (task.stdin_file && typeof task.stdin_file == 'string' && task.stdin_file.length > 0) {
			instream = fs.openReadFileStream(task.stdin_file);
			if (!instream) {
				onFinalError('error opening stdin file ' + task.stdin_file);
				return;
			}
		}
		var exec = require('child_process').exec;
		var options = {
		};
		//console.log('cmd=' + task.cmd);
		var child = exec(task.cmd, options);
		if (instream && child.stdin) instream.pipe(child.stdin);
		task.pid = child.pid
		onFinalReturn(task);
		markTaskStartDB(conn_str, lp.db.sqls['MarkTaskStart'], task);
		var stdout = '';
		var stderr = '';
		child.stdout.on('data', function(data){
			stdout += data.toString();
		});
		child.stderr.on('data', function(data){
			stderr += data.toString();
		});
		child.on('error', function(err) {
			task.ret_code = err.code;
			console.log('child.on_error: ret=' + task.ret_code);
			markTaskFinishedDB(conn_str, lp.db.sqls['MarkTaskEnd'], task, stdout, stderr, notifyDispatcherOnTaskFinished, console.log);
		});
		child.on('close', function(exitCode) {
			task.ret_code = exitCode;
			console.log('child.on_close: ret=' + task.ret_code);
			markTaskFinishedDB(conn_str, lp.db.sqls['MarkTaskEnd'], task, stdout, stderr, notifyDispatcherOnTaskFinished, console.log);
		});
	}
	,onFinalError);
}

function handleKillProcessTree(request, result) {
	function onFinalReturn() {
		result.json({});
	}
	var processid = request.body.pid;
	console.log('killing the process pid=' + processid + ' and its childern...');
	var kill = require('tree-kill');
	kill(processid, 'SIGKILL');
	onFinalReturn();
}

router.use(function timeLog(req, res, next) {
	//console.log('an incomming request @ /grid_task_launcher. Time: ', Date.now());
	next();
});

router.post('/LAUNCH_TASK', handleLaunchTask);
router.post('/KILL_PROCESS_TREE', handleKillProcessTree);

module.exports = router;