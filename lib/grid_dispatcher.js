// grid dispatcher
var sqlserver = require('msnodesql');
var http = require('http');
var url = require('url');

// some constants
var ON_TASK_FINISHED_PATH = '/';
var ON_TASK_FINISHED_HTTP_METHOD = 'POST';
var LAUNCHER_LAUNCH_METHOD = 'LAUNCH_TASK';
var LAUNCHER_KILL_PROCESS_TREE_METHOD = 'KILL_PROCESS_TREE';

function task_toString(task) {return 'task{' + task.job_id + ',' + task.index + '}';}
function make_err_json(err)
{
	var o = {exception: err.toString()};
	return JSON.stringify(o);
}

// the task dispatcher class
// events supported:
// 1. onNodesStatusChanged(event)
// 2. onQueueChanged(event)
// 3. onNewJobSubmitted(job_id, submitToken)
// 4. onJobStatusChanged(job_progress)
// 5. onJobRemovedFromTracking(job_id)
// 6. onTaskCompleted(task)
function Dispatcher(task_monitor_port, launcher_url_home_path, db_conn, nodes) {
	var me = this;
	
	function notifyNodesStatusChanged(event) {if (typeof me.onNodesStatusChanged === 'function') me.onNodesStatusChanged(event);}
	function notifyQueueChanged(event) {if (typeof me.onQueueChanged === 'function') me.onQueueChanged(event);}
	function notifyNewJobSubmitted(job_id, submitToken) {if (typeof me.onNewJobSubmitted === 'function') me.onNewJobSubmitted(job_id, submitToken);}
	function notifyJobStatusChanged(job_progress) {if (typeof me.onJobStatusChanged === 'function') me.onJobStatusChanged(job_progress);}
	function notifyJobRemovedFromTracking(job_id) {if (typeof me.onJobRemovedFromTracking === 'function') me.onJobRemovedFromTracking(job_id);}
	function notifyTaskCompleted(task) {if (typeof me.onTaskCompleted === 'function') me.onTaskCompleted(task);}
	
	// node class
	function Node(name, num_cpus, ip, use_ip, port, description) {
		this.name = name;
		this.num_cpus = num_cpus;
		this.use_ip = (typeof use_ip != "boolean" ? true : use_ip);
		if (this.use_ip && (!ip || ip.length == 0)) throw 'node missing IP address';
		this.ip = ip;
		this.port = port;
		this.description = (typeof description != "string" ? name : description);
		this.getAddress = function() {return (this.use_ip ? this.ip : this.name) + ":" + this.port;};
	}
	// cpu class
	function CPU(node) {
		this.getNode = function () {return node;};
	}
	// nodes status class
	// events supported:
	// 1. onStatusChanged(event)
	function NodesStatus(nodes) {
		var self = this;
		var __nodes = {};
		function notifyStatusChanged() {if (typeof self.onStatusChanged == 'function') self.onStatusChanged(self.toObject());}
		function getNodeStatus(pr) {
			var nd = pr[0];
			var ns = {name: nd.name, num_cpus: nd.num_cpus, use_ip: nd.use_ip, port: nd.port, description: nd.description, available_cpus: pr[1]};
			if (nd.ip && nd.ip.length > 0) ns.ip = nd.ip;
			return ns;			
		}
		this.incrementAvailableCPUCount = function(host) {
			if (__nodes[host]) {
				__nodes[host][1]++;
				notifyStatusChanged();
			}
		};
		this.decrementAvailableCPUCount = function(host) {
			if (__nodes[host] && __nodes[host][1] > 0) {
				__nodes[host][1]--;
				notifyStatusChanged();
			}
		};
		this.removeNode = function(host) {
			if (__nodes[host]) {
				delete __nodes[host];
				notifyStatusChanged();
			}
		};
		// add a node to the grid
		// node is of type Node
		this.addNode = function(node) {
			var host = node.name;
			if (!__nodes[host]) {
				__nodes[host] = [node, node.num_cpus];
				notifyStatusChanged();
			}
		};
		this.getNode = function(host) {
			return (__nodes[host] ? __nodes[host][0] : null);
		};
		//return null if no cpu available
		this.getAvailableCPUs = function() {
			var ret = [];
			for (var host in __nodes) {
				var nd = __nodes[host];
				var node = nd[0];
				var num_avail_cpus = nd[1];
				for (var i = 0; i < num_avail_cpus; i++)
					ret.push(new CPU(node));
			}
			return (ret.length == 0 ? null : ret);
		};
		this.toObject = function() {
			var o = {num_nodes: 0, total_cpus:0, available_cpus: 0, nodes:{}};
			for (var host in __nodes) {
				o.num_nodes++;
				var ns = getNodeStatus(__nodes[host]);
				o.total_cpus += ns.num_cpus;
				o.available_cpus += ns.available_cpus;
				o.nodes[host] = ns;
			}
			if (o.total_cpus > 0 && o.total_cpus >= o.available_cpus) o.utilization = parseFloat(o.total_cpus - o.available_cpus)*100.0/parseFloat(o.total_cpus);
			return o;
		};
		for (var host in nodes)	{
			var node = nodes[host];
			__nodes[host] = [new Node(host, node.num_cpus, node.ip, node.use_ip, node.port, node.description), node.num_cpus];
		}
		notifyStatusChanged();
	}
	// queue class
	// events supported: 
	// 1. onChanged(event)
	function Queue() {
		var self = this;
		var __queues = {};	// the internal queue, map from job id to tasks
		function notifyChanged() {if (typeof self.onChanged == 'function') self.onChanged(self.toObject());}
		this.enqueueJobTasks = function(job) {
			__queues[job.id] = job.tasks;
			notifyChanged();
		};
		// return all job ids in the queue, null if no jobs in the queue
		this.getJobIDs = function() {
			var ret = [];
			for (var jobid in __queues)
				ret.push(jobid);
			return (ret.length > 0 ? ret : null);
		};
		this.isEmpty = function () {return (this.getJobIDs() != null);};
		// dequeuing tasks. returns null if no tasks can be dequeue (empty)
		this.dequeueTasks = function(max_tasks_to_dequeue) {
			// max_tasks_to_dequeue > 0
			var jobIds = this.getJobIDs();
			if (!jobIds) return null;	// queue is empty
			var numTasksLeft = max_tasks_to_dequeue;
			var tasks = [];
			while(numTasksLeft > 0 && jobIds.length > 0)
			{
				var num = Math.floor((Math.random()*jobIds.length)+1); 
				var jobIndex = num-1;
				var jobId = jobIds[jobIndex];
				tasks.push(__queues[jobId][0]);
				__queues[jobId].shift();	// pop the head
				if (__queues[jobId].length == 0)
				{
					delete __queues[jobId];
					jobIds.splice(jobIndex,1);
				}
				numTasksLeft--;
			}
			if (tasks.length > 0) notifyChanged();
			return (tasks.length > 0 ? tasks : null);
		};
		this.removeJobTasksFromQueue = function(jobid) {
			if (__queues[jobid]) {
				delete __queues[jobid];
				notifyChanged();
			}
		};
		this.clear = function() {
			__queues = {};
			notifyChanged();
		};
		this.toObject = function() {
			var o = {count: 0, num_jobs: 0, count_by_jobid:{}};
			var count_by_jobid = {};
			for (var jobid in __queues)	{
				o.num_jobs++;
				o.count += __queues[jobid].length;
				o.count_by_jobid[jobid] = __queues[jobid].length;
			}
			return o;
		};
	}
	
	function dataRowToJobProgress(row) {
		var job_progress = {job_id: null, status: '', num_tasks: 0, num_tasks_finished: 0, complete_pct: 0.0, success: false};
		job_progress.job_id = row['job_id'];
		if (row['description']) job_progress.description = row['description'];
		if (row['cookie']) job_progress.cookie = row['cookie'];
		job_progress.status = row['status'];
		job_progress.num_tasks = row['num_tasks'];
		job_progress.num_tasks_finished = row['num_tasks_finished'];
		job_progress.complete_pct = row['complete_pct'];
		job_progress.success = row['success'];
		return job_progress;
	}
	// jobs tracking class
	// events supported:
	// 1. onNewJobSubmitted(job_id, submitToken)
	// 2. onJobStatusChanged(job_progress)
	// 3. onJobRemoved(job_id)
	function TrackedJobs(max_done_jobs_tracking) {
		var self = this;
		var __jobs = {};	// map from job_id to job_progress
		var __doneJobs = [];	// array of finished job ids
		function notifyNewJobSubmitted(job_id, submitToken) {if (typeof self.onNewJobSubmitted == 'function') self.onNewJobSubmitted(job_id, submitToken);}
		function notifyJobStatusChanged(job_progress) {if (typeof self.onJobStatusChanged == 'function') self.onJobStatusChanged(job_progress);}
		function notifyJobRemoved(job_id) {if (typeof self.onJobRemoved == 'function') self.onJobRemoved(job_id);}
		this.trackNewJob = function(job_id, description, cookie, num_tasks, submitToken) {
			var job_progress = {job_id: job_id, status:'SUBMITTED', num_tasks: num_tasks, num_tasks_finished: 0, complete_pct: 0.0, success: false};
			if (description) job_progress.description = description;
			if (cookie) job_progress.cookie = cookie;
			__jobs[job_id] = job_progress
			notifyNewJobSubmitted(job_id, submitToken);
			notifyJobStatusChanged(job_progress);
		};
		function isJobDone(status) {return (status == 'ABORTED' || status == 'FINISHED');}
		this.updateJobProgress = function(job_progress) {
			var job_id = job_progress.job_id;
			var old_job_progress = __jobs[job_id];
			if (!old_job_progress) return;
			if (isJobDone(old_job_progress.status)) return;	// job is already done
			if (old_job_progress.status == 'STATRED' && job_progress.status == 'SUBMITTED') return;
			if (old_job_progress.status != job_progress.status) {	// job status changed
				old_job_progress.status = job_progress.status;
				old_job_progress.num_tasks_finished = job_progress.num_tasks_finished;
				old_job_progress.complete_pct = job_progress.complete_pct;
				notifyJobStatusChanged(job_progress);
				if (isJobDone(job_progress.status)) {
					__doneJobs.push(job_id);
					if (__doneJobs.length > max_done_jobs_tracking) {
						var job_id_removed =__doneJobs[0];
						__doneJobs.shift();	// pop the head
						if (__jobs[job_id_removed]) {
							delete __jobs[job_id_removed];
							notifyJobRemoved(job_id_removed);
						}
					}
				}
			}
			else {	// status did not change
				if (old_job_progress.num_tasks_finished < job_progress.num_tasks_finished) {
					old_job_progress.num_tasks_finished = job_progress.num_tasks_finished;
					old_job_progress.complete_pct = job_progress.complete_pct;
					notifyJobStatusChanged(job_progress);
				}
			}
		};
		this.toObject = function() {
			var o = {};
			for (var job_id in __jobs)
				o[job_id] = __jobs[job_id];
			return o;
		};
		function getLatestJobsStatusDB(onDone) {
			var sql = db_conn.sqls['GetLatestJobsStatus'];
			sqlserver.query(db_conn.conn_str, sql, onDone);
		}
		// load from the database
		getLatestJobsStatusDB(function(err, dataTable) {
			if (err) {
				console.error('!!! Error: ' + err.toString());
			}
			else {
				for (var i in dataTable) {	// for each row
					var job_progress = dataRowToJobProgress(dataTable[i]);
					if (isJobDone(job_progress.status)) {
						if (__doneJobs.length < max_done_jobs_tracking) {
							__doneJobs.push(job_progress.job_id);
							__jobs[job_progress.job_id] = job_progress;
						}
					}
					else
						__jobs[job_progress.job_id] = job_progress;
				}
				__doneJobs.sort(function(a, b){return a-b});	// sort by ascending order
			}
		});
	}
	var ns = new NodesStatus(nodes);		// nodes status
	var queue = new Queue();				// tasks queue
	var trackedJobs = new TrackedJobs(20);	// tracked jobs
	var dispatching = false;				// dispatching flag
	var shutdown = false;					// shutdown flag
	
	ns.onStatusChanged = function(event) {
		if (typeof event.utilization == 'number')
			console.log('<grid utilization>: ' + event.utilization.toFixed(2) + '%');
		notifyNodesStatusChanged(event);
	};
	queue.onChanged = function(event) {
		console.log('<<num tasks in queue>>: ' + event.count);
		notifyQueueChanged(event);
	};
	trackedJobs.onNewJobSubmitted = function (job_id, submitToken) {
		notifyNewJobSubmitted(job_id, submitToken);
	};
	trackedJobs.onJobStatusChanged = function (job_progress) {
		if (job_progress.status == 'FINISHED')
			console.log('DETECTED: job ' + job_progress.job_id + ' is finished');
		else if (job_progress.status == 'ABORTED')
			console.log('DETECTED: job ' + job_progress.job_id + ' is aborted');
		else
			console.log('job ' + job_progress.job_id + ': ' + job_progress.num_tasks_finished + '/' + job_progress.num_tasks + '=' + job_progress.complete_pct.toFixed(2)+'%');
		notifyJobStatusChanged(job_progress);
	};
	trackedJobs.onJobRemoved = function (job_id) {
		console.log('job ' + job_id + ' is removed from tracking');
		notifyJobRemovedFromTracking(job_id);
	};
	
	// create an HTTP server listening for task finished event
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	function handleOnTaskFinished(request, body, result) {
		result.writeHead(200, {'Content-Type': 'application/json'});
		try	{
			var task = JSON.parse(body);
			console.log(task_toString(task) + " finished running with ret_code=" + task.ret_code);
			me.onTaskFinished(task);
			result.end('{}');
		} catch(e) {
			console.error("bad body: " + body);
			result.end(make_err_json(e));
		}
	}
	http.createServer(function (request, result) {
		//console.log(request.headers);
		var parse = url.parse(request.url, true);
		//console.log(parse);
		var body = '';
		request.on('data', function(chunk) {
			body += chunk;
		});
		request.on('end', function() {
			handleOnTaskFinished(request, body, result);
		});
	}).listen(task_monitor_port);
	console.log('task monitor running at port ' + task_monitor_port);
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	function randomlyChooseCPUs(availableCPUs, numToPick) {
		// pre-condition:
		// 1. availableCPUs.length > 0
		// 2. numToPick > 0
		// 3. availableCPUs.length >= numToPick
		
		// re-organize cpus by node name
		/////////////////////////////////////////////////////////////////////
		var cpusByNodeName = {};
		for (var i in availableCPUs)	// for each available cpu
		{
			var cpu = availableCPUs[i];
			var node = cpu.getNode();
			var nodeName = node.name;
			if (typeof cpusByNodeName[nodeName] == "undefined") cpusByNodeName[nodeName] = [];
			cpusByNodeName[nodeName].push(cpu);
		}
		/////////////////////////////////////////////////////////////////////
		
		// get all the unique node names
		/////////////////////////////////////////////////////////////////////
		var nodeNames = [];
		for (var nodeName in cpusByNodeName)
			nodeNames.push(nodeName);
		/////////////////////////////////////////////////////////////////////
		
		// randomly shuffle the node names
		nodeNames.sort(function() {return 0.5 - Math.random()});
		
		var cpusPicked = [];
		var iter = 0;	// iterator over the node names array
		var i = numToPick;
		while (i > 0)
		{
			var nodeName = nodeNames[iter];
			if (cpusByNodeName[nodeName].length > 0)
			{
				var cpu = cpusByNodeName[nodeName].shift();
				cpusPicked.push(cpu);
				i--;
			}
			iter++;
			if (iter == nodeNames.length) iter = 0;
		}
		return cpusPicked;
	}
	
	function dispatchTasksIfNecessary() {
		var availableCPUs = null;
		var tasks = null;
		if (!dispatching && (availableCPUs=ns.getAvailableCPUs()) && (tasks=queue.dequeueTasks(availableCPUs.length))) {
			dispatching = true;
			//assert(availableCPUs.length>0 && tasks.length > 0 && availableCPUs.length >= tasks.length);
			var cpusSelected = randomlyChooseCPUs(availableCPUs, tasks.length);
			//assert(cpusSelected.length == tasks.length);
			var numResponsesOutstanding = tasks.length;
			function onDispatchAck(i, host, processid, error) {
				numResponsesOutstanding--;
				if (error != null) console.log('!!! request[' + i + ']: ' + error.toString());
				console.log('check in response for request[' + i + ']. ' + numResponsesOutstanding + ' request(s) still outstanding');
				if (processid > 0)
					ns.decrementAvailableCPUCount(host);
				else
				{
					//log error for host
				}
				if (!numResponsesOutstanding) {
					console.log('no more outstanding request :-)');
					dispatching = false;
					dispatchTasksIfNecessary();
				}
			}
			function getHTTPRspHandler(i, host)	{
				return (function(res) {
					//console.log("statusCode: ", res.statusCode);
					//console.log("headers: ", res.headers);
					if (res.statusCode != 200)
					{
						onDispatchAck(i, host, 0, 'http returns status code of ' + res.statusCode);
						return;
					}
					res.on('data', function(d) {
						try	{
							var o = JSON.parse(d.toString());
							if (typeof o.exception == 'string') throw 'launcher retuns exception: ' + o.exception;
							var proessid = o.pid;
							if (isNaN(proessid) || proessid <= 0) throw 'launcher returns bad process id';
							console.log('process with pid='+ proessid + ' was launched on ' + host);
							onDispatchAck(i, host, proessid, null);
						}
						catch(e) {
							onDispatchAck(i, host, 0, e.toString());
						}
					});
				});
			}
			function getErrorHandler(i, host) {
				return (function(e) {
					onDispatchAck(i, host, 0, 'problem with request: ' + e.message);
				});
			}
			console.log("sending " + tasks.length + ' http request(s) to dispatch task(s)...');
			var options = {
				path: launcher_url_home_path + LAUNCHER_LAUNCH_METHOD + '/'
				,method: 'POST'
				,headers: {
					'Content-Type': 'application/json'
				}
			};
			for (var i in cpusSelected)	{
				var task = tasks[i];
				var cpu = cpusSelected[i];
				var node = cpu.getNode();
				var host = node.name;
				task.node = host;
				console.log('requests[' + i.toString() + ']: ' + task_toString(task) + ' ---> ' + host);
				var params = {
					db:	{
						conn_str: db_conn.conn_str
						,sqls: {
							'GetTaskDetail': db_conn.sqls['GetTaskDetail']
							,'MarkTaskStart': db_conn.sqls['MarkTaskStart']
							,'MarkTaskEnd': db_conn.sqls['MarkTaskEnd']
						}
					}
					,otf: {
						port: task_monitor_port
						,path: ON_TASK_FINISHED_PATH
						,method: ON_TASK_FINISHED_HTTP_METHOD
					}
					,task: task
				};
				var s = JSON.stringify(params);
				options.hostname = (node.use_ip ? node.ip : node.name);
				options.port = node.port;
				options.headers['Content-Length'] = s.length;
				var req = http.request(options, getHTTPRspHandler(i, host));
				req.on('error', getErrorHandler(i, host));
				req.end(s);
			}
		}
	}
	// submit a job to the database
	function submitJobDB(job_xml, onDone) {
		function callErrorHandler(err) {
			if (typeof onDone === 'function') onDone(err, null);
		}
		try {
			sqlserver.query(db_conn.conn_str, db_conn.sqls['SubmitJob'], [job_xml], function(err, dataTable) {
				if (err)
					callErrorHandler(err);
				else {
					if (dataTable.length > 0) {
						var job_id = dataTable[0]['job_id'];
						job_id = (job_id ? parseInt(job_id.toString()) : null);
						if (isNaN(job_id))
							callErrorHandler('bad new job id');
						else {
							if (typeof onDone === 'function') onDone(null, job_id);
						}
					}
					else
						callErrorHandler('unable to submit job');
				}
			});
		} catch (e) {
			callErrorHandler(e);
		}
	}
	// submit a job
	this.submitJob = function(job, job_xml, submitToken, onJobCreated, onError) {
		if (shutdown) {
			if (typeof onError == 'function') onError('grid has shutdown');
			return;
		}
		if (job.tasks.length == 0) {
			if (typeof onError == 'function') onError('no task for job');
			return;
		}
		submitJobDB(job_xml, function(err, job_id) {
			if (err) {
				if (typeof onError === 'function') onError(err.toString());
			}
			else {
				console.log('job ' + job_id + " created");
				trackedJobs.trackNewJob(job_id, job.description, job.cookie, job.tasks.length, submitToken);
				if (typeof onJobCreated === 'function') onJobCreated(job_id);
				job.id = job_id;
				for (var i in job.tasks)
					job.tasks[i].job_id = job_id;
				// enqueue the job tasks and trigger dispatch if necessary
				queue.enqueueJobTasks(job);
				dispatchTasksIfNecessary();				
			}
		});
	};
	// get job progress from the database
	function getJobStatusDB(job_id, onDone, onError) {
		sqlserver.query(db_conn.conn_str, db_conn.sqls['GetJobStatus'], [job_id], function(err, dataTable) {
			if (err) {
				if (typeof onError === 'function') onError(err.toString());
			}
			else {
				if (dataTable.length < 1) {
					if (typeof onError === 'function') onError('bad job id: ' + job_id);
				}
				else {
					if (typeof onDone === 'function') onDone(dataRowToJobProgress(dataTable[0]));
				}
			}
		});
	}
	
	// launcher web-calls this function when a task is finished
	this.onTaskFinished = function(task) {
		ns.incrementAvailableCPUCount(task.node);
		dispatchTasksIfNecessary();
		
		notifyTaskCompleted(task);
		
		getJobStatusDB(task.job_id
		,function(job_progress) {
			trackedJobs.updateJobProgress(job_progress);
		}
		,function (err) {
			console.error('!!! Error: ' + err.toString());
		});
	};
	// kill a job
	this.killJob = function(job_id, onDone, onError) {
		queue.removeJobTasksFromQueue(job_id);
		var MAX_POLLS = 5;
		var INTER_POLL_INTERVAL_MS = 1000;
		var num_polls = 0;
		function pollRunningTasksForJobDB(abort, onDone, onError) {
			num_polls++;
			if (abort)
				console.log('KILL: aborting job ' + job_id + ', poll #' + num_polls);
			else
				console.log('KILL: performing job task poll for job ' + job_id + ', poll #' + num_polls);
			var sql = (abort ? db_conn.sqls['AbortJob'] : db_conn.sqls['GetJobRunningTasks']);
			var stmt = sqlserver.query(db_conn.conn_str, sql, [job_id]);
			var ci = null;
			var runningTasks = null;
			var task = {};
			stmt.on('meta', function (meta) {ci = meta;});
			stmt.on('row', function (idx) {
				if (!runningTasks) runningTasks = [];
				task = {};
				runningTasks.push(task);
			});
			stmt.on('column', function (idx, data, more) {
				var fld = ci[idx].name;
				switch(fld)
				{
					case 'job_id':
						task.job_id = data;
						break;
					case 'index':
						task.index = data;
						break;
					case 'node':
						task.node = data;
						break;
					case 'pid':
						task.pid = data;
						break;
				}
			});
			stmt.on('done', function ()	{
				if (typeof onDone == 'function') onDone(runningTasks);
			});
			stmt.on('error', function (err) {
				if (typeof onError == 'function') onError(err.toString());
			});
		}		
		function dispatchKillRunningTasks(runningTasks)	{
			var options = {
				path: launcher_url_home_path + LAUNCHER_KILL_PROCESS_TREE_METHOD + '/'
				,method: 'POST'
				,headers: {
					'Content-Type': 'application/json'
				}
			};
			for (var i in runningTasks)	{
				var task = runningTasks[i];
				var host = task.node;
				var node = ns.getNode(host);
				if (node) {
					var processid = task.pid;
					var params = {pid: processid};
					var s = JSON.stringify(params);
					options.hostname = (node.use_ip ? node.ip : node.name);
					options.port = node.port;
					options.headers['Content-Length'] = s.length;
					//console.log(options);
					var req = http.request(options);
					req.end(s);
				}
			}
		}
		function onPollingFinsihed(runningTasks) {
			function scheduleNextPollIfNotMaxout() {
				if (num_polls < MAX_POLLS) {
					console.log('KILL: scheduling next poll for job ' + job_id + ' in ' + INTER_POLL_INTERVAL_MS + ' ms');
					setTimeout(function() {pollRunningTasksForJobDB(false, onPollingFinsihed, onError);}, INTER_POLL_INTERVAL_MS);
				}
				else {	// polls maxout
					console.log('KILL: kill process for job ' + job_id + ' is completed');
					onDone();
				}
			}
			if (runningTasks)	// there are still running tasks
			{
				console.log('KILL: ' + runningTasks.length + ' task(s) still running for job ' + job_id + '. dispatching kill requests...');
				//console.log(JSON.stringify(runningTasks));
				dispatchKillRunningTasks(runningTasks);
				scheduleNextPollIfNotMaxout();
			}
			else	// no tasks running
			{
				console.log('KILL: :-) no task running detected for job ' + job_id);
				scheduleNextPollIfNotMaxout();
			}
		}
		pollRunningTasksForJobDB(true, onPollingFinsihed, onError);
	};
	
	this.getJobProgress = function(job_id, onDone) {
		getJobStatusDB(job_id
		,function (job_progress) {
			if (typeof onDone === 'function') onDone(null, job_progress);
		}, function(err) {
			if (typeof onDone === 'function') onDone(err, null);
		});
	};
	this.getJobResult = function(job_id, onDone) {
		var sql = db_conn.sqls['GetJobResult'];
		sqlserver.query(db_conn.conn_str, sql, [job_id], onDone);
	};
	this.getGridState = function() {return {"nodesStatus": ns.toObject(), "queue": queue.toObject(), "trackedJobs": trackedJobs.toObject()};};
}

module.exports=Dispatcher;