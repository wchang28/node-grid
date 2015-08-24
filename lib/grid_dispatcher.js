// grid dispatcher
var sqlserver = require('msnodesql');
var http = require('http');
var url = require('url');
require('date-utils');

function task_toString(task) {return 'task{' + task.job_id + ',' + task.index + '}';}
function make_err_json(err) {return JSON.stringify({exception: err.toString()});}

// the task dispatcher class
// events supported:
// 1. onNodesStatusChanged(event)
// 2. onNodeAdded(newNode)
// 3. onNodeEnabled(node)
// 4. onNodeDisabled(node, leaveGrid)
// 5. onNodeRemoved(nodeRemoved)
// 6. onQueueChanged(event)
// 7. onNewJobSubmitted(job_id, submitToken)
// 8. onJobStatusChanged(job_progress)
// 9. onJobRemovedFromTracking(job_id)
// 10. onTaskCompleted(task)
function Dispatcher(msgBroker, dispatcherToTaskLauncherTopic, db_conn) {
	var me = this;
	
	function notifyNodesStatusChanged(event) {if (typeof me.onNodesStatusChanged === 'function') me.onNodesStatusChanged(event);}
	function notifyNodeAdded(newNode) {if (typeof me.onNodeAdded === 'function') me.onNodeAdded(newNode);}
	function notifyNodeEnabled(node) {if (typeof me.onNodeEnabled === 'function') me.onNodeEnabled(node);}
	function notifyNodeDisabled(node, leaveGrid) {if (typeof me.onNodeDisabled === 'function') me.onNodeDisabled(node, leaveGrid);}
	function notifyNodeRemoved(nodeRemoved) {if (typeof me.onNodeRemoved === 'function') me.onNodeRemoved(nodeRemoved);}
	function notifyQueueChanged(event) {if (typeof me.onQueueChanged === 'function') me.onQueueChanged(event);}
	function notifyNewJobSubmitted(job_id, submitToken) {if (typeof me.onNewJobSubmitted === 'function') me.onNewJobSubmitted(job_id, submitToken);}
	function notifyJobStatusChanged(job_progress) {if (typeof me.onJobStatusChanged === 'function') me.onJobStatusChanged(job_progress);}
	function notifyJobRemovedFromTracking(job_id) {if (typeof me.onJobRemovedFromTracking === 'function') me.onJobRemovedFromTracking(job_id);}
	function notifyTaskCompleted(task) {if (typeof me.onTaskCompleted === 'function') me.onTaskCompleted(task);}
	
	// node should have the following fields:
	// 1. name
	// 2. ip
	// 3. port
	// 4. use_ip
	// 5. num_cpus
	
	// cpu class
	function CPU(node) {
		this.getNode = function () {return node;};
	}
	
	// nodeItem should have the following fields:
	// 1. node
	// 2. available_cpus
	// 3. active
	// 4. leavePending
	
	// nodes status class
	// events supported:
	// 1. onStatusChanged(event)
	// 2. onMoreCPUsAvailable()
	// 3. onNodeAdded(node)
	// 4. onNodeEnabled(node)
	// 5. onNodeDisabled(node, leaveGrid)
	// 6. onNodeRemoved(node)
	function NodesStatus() {
		var self = this;
		var __nodes = {};
		function notifyStatusChanged() {if (typeof self.onStatusChanged == 'function') self.onStatusChanged(self.toObject());}
		function notifyMoreCPUsAvailable() {if (typeof self.onMoreCPUsAvailable == 'function') self.onMoreCPUsAvailable();}
		function notifyNodeAdded(newNode) {if (typeof self.onNodeAdded == 'function') self.onNodeAdded(newNode);}
		function notifyNodeEnabled(node) {if (typeof self.onNodeEnabled == 'function') self.onNodeEnabled(node);}
		function notifyNodeDisabled(node, leaveGrid) {if (typeof self.onNodeDisabled == 'function') self.onNodeDisabled(node, leaveGrid);}
		function notifyNodeRemoved(nodeRomoved) {if (typeof self.onNodeRemoved == 'function') self.onNodeRemoved(nodeRomoved);}
		function getNodeStatus(nodeItem) {
			var node = nodeItem.node;
			var ns =
			{
				name: node.name
				,ip: node.ip
				,port: node.port
				,use_ip: node.use_ip
				,num_cpus: node.num_cpus
				,available_cpus: nodeItem.available_cpus
				,active: nodeItem.active
				,leavePending: nodeItem.leavePending
			};
			return ns;			
		}
		function nodeIdle(nodeItem) {
			return (nodeItem.available_cpus == nodeItem.node.num_cpus);
		}
		this.incrementAvailableCPUCount = function(host) {
			var nodeItem = __nodes[host];
			if (nodeItem) {
				nodeItem.available_cpus++;
				notifyMoreCPUsAvailable();
				notifyStatusChanged();
			}
		};
		this.decrementAvailableCPUCount = function(host) {
			var nodeItem = __nodes[host];
			if (nodeItem && nodeItem.available_cpus > 0) {
				nodeItem.available_cpus--;
				notifyStatusChanged();
			}
		};
		// add/join a node to the grid
		this.addNode = function(node) {
			var host = node.name;
			if (!__nodes[host]) {
				__nodes[host] = {node: node, available_cpus: node.num_cpus, active: true, leavePending: false};
				notifyNodeAdded(node);
				notifyStatusChanged();
				return [true, null];
			} else
				return [false, 'node already in grid'];
		};
		// enable node
		this.enableNode = function(host) {
			var nodeItem = __nodes[host];
			if (nodeItem && !nodeItem.active) {
				nodeItem.active = true;
				nodeItem.leavePending = false;
				notifyNodeEnabled(nodeItem.node);
				notifyStatusChanged();
			}
		};
		// disable a node
		// if leaveGrid is true, then the intent is to have node leave the grid after it become idle
		this.disableNode = function(host, leaveGrid) {
			if (typeof leaveGrid !== 'boolean') leaveGrid = false;
			var nodeItem = __nodes[host];
			if (nodeItem && nodeItem.active) {
				nodeItem.active = false;
				nodeItem.leavePending = leaveGrid;
				notifyNodeDisabled(nodeItem.node, leaveGrid);
				notifyStatusChanged();
			}
		};
		this.removeNode = function(host) {
			var nodeItem = __nodes[host];
			if (nodeItem) {
				if (!nodeIdle(nodeItem)) console.error('!!! Error: node ' + nodeItem.node.name + ' is not idle before leaving the grid');
				delete __nodes[host];
				notifyNodeRemoved(nodeItem.node);
				notifyStatusChanged();
			}
		};
		this.getNode = function(host) {
			return (__nodes[host] ? __nodes[host].node : null);
		};
		this.getNodeByIPAddress = function(ipAddress) {
			for (var host in __nodes) {
				var nodeItem = __nodes[host];
				var node = nodeItem.node;
				if (node.ip === ipAddress)
					return node;
			}
			return null;
		};
		//return null if no cpu available
		this.getAvailableCPUs = function() {
			var ret = [];
			for (var host in __nodes) {	// for each host
				var nodeItem = __nodes[host];
				if (nodeItem.active) {
					var node = nodeItem.node;
					var num_avail_cpus = nodeItem.available_cpus;
					for (var i = 0; i < num_avail_cpus; i++)
						ret.push(new CPU(node));
				}
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
		notifyStatusChanged();
	}
	// queue class
	// events supported: 
	// 1. onChanged(event)
	// 2. onTasksEnqueued(tasks)
	function Queue() {
		var self = this;
		var __queues = {};	// the internal queue, map from job id to tasks
		function notifyChanged() {if (typeof self.onChanged == 'function') self.onChanged(self.toObject());}
		function notifyTasksEnqueued(tasks) {if (typeof self.onTasksEnqueued == 'function') self.onTasksEnqueued(tasks);}
		this.enqueueJobTasks = function(job) {
			__queues[job.id] = job.tasks;
			notifyTasksEnqueued(job.tasks);
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
	function correctDBTime(dbTime) {
		var d = new Date(dbTime.getUTCFullYear(), dbTime.getUTCMonth(), dbTime.getUTCDate(), dbTime.getUTCHours(), dbTime.getUTCMinutes(), dbTime.getUTCSeconds(), dbTime.getUTCMilliseconds());
		return d;
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
		job_progress.submit_time = correctDBTime(row['submit_time']).toFormat('YYYY-MM-DD HH24:MI:SS');
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
			var job_progress = {job_id: job_id, status:'SUBMITTED', num_tasks: num_tasks, num_tasks_finished: 0, complete_pct: 0.0, success: false, submit_time: new Date().toFormat('YYYY-MM-DD HH24:MI:SS')};
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
				old_job_progress.success = job_progress.success;
				old_job_progress.submit_time = job_progress.submit_time;
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
					old_job_progress.success = job_progress.success;
					old_job_progress.submit_time = job_progress.submit_time;
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
		// load latest jobs from the database
		console.log('loading latest jobs from the database...');
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
				console.log('latest jobs are loaded');
			}
		});
	}
	
	function notifyNodeAcceptTasks(node, accept, leaveGrid) {
		var content = {accept: accept};
		if (!accept && typeof leaveGrid === 'boolean') content.leaveGrid = leaveGrid;
		var retObj = {method: "nodeAcceptTasks", host: node.name, content: content};
		msgBroker.send(dispatcherToTaskLauncherTopic, {persistent: true}, JSON.stringify(retObj), function(receipt_id) {});
	}
	
	var ns = new NodesStatus();				// nodes status
	var queue = new Queue();				// tasks queue
	var trackedJobs = new TrackedJobs(100);	// tracked jobs
	var numResponsesOutstanding = 0;		// number of responses outstanding, also serve as the dispatching flag
	var shutdown = false;					// shutdown flag
	this.isDispatchingTasks = function() {return (numResponsesOutstanding > 0);}
	
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
	
	// the main task dispatcher function
	function dispatchTasksIfNecessary() {
		var availableCPUs = null;
		var tasks = null;
		//console.log('dispatchTasksIfNecessary(), numResponsesOutstanding=' + numResponsesOutstanding + ', availableCPUs=' + (ns.getAvailableCPUs() ? ns.getAvailableCPUs().length : '(null)'));
		if (!me.isDispatchingTasks() && (availableCPUs=ns.getAvailableCPUs()) && (tasks=queue.dequeueTasks(availableCPUs.length))) {
			//assert(availableCPUs.length>0 && tasks.length > 0 && availableCPUs.length >= tasks.length);
			var cpusSelected = randomlyChooseCPUs(availableCPUs, tasks.length);
			//assert(cpusSelected.length == tasks.length);
			numResponsesOutstanding = tasks.length;	// numResponsesOutstanding > 0 ==> dispatching
			var tasksByHost = {};	// tasks by host
			for (var i in cpusSelected)	 {// for each cpu
				var task = tasks[i];
				var cpu = cpusSelected[i];
				var node = cpu.getNode();
				var host = node.name;
				task.node = host;
				if (!tasksByHost[host]) tasksByHost[host] = {node: node, tasks: []};
				tasksByHost[host].tasks.push(task);
			}
			var options = {
				path: '/grid_task_launcher/dispatched_tasks'
				,method: 'POST'
				,headers: {
					'Content-Type': 'application/json'
				}
			}
			function getHTTPRspHandler(host) {
				return (function(res) {
					res.setEncoding('utf8');
					var s = '';
					res.on('data', function(d) {
						s += d.toString();
					});
					res.on('end', function() {
						if (res.statusCode != 200)
							console.error("!!! " + host + ': http returns status code of ' + res.statusCode);
						else
							console.error(host + ' received the tasks dispatch: ' + s);
					})
				});				
			}
			function getHTTPErrorHandler(host) {
				return (function(err) {
					console.error("!!! " + host + ' has problem with the HTTP request: ' + err.toString());
				});				
			}
			for (var host in tasksByHost) {	// for each node
				var node = tasksByHost[host].node;
				var tasks = tasksByHost[host].tasks;
				options.hostname = (node.use_ip ? node.ip : node.name);
				options.port = node.port;
				var s = JSON.stringify(tasks);
				options.headers['Content-Length'] = s.length;
				var req = http.request(options, getHTTPRspHandler(host));
				req.on('error', getHTTPErrorHandler(host));
				req.end(s);
			}
		}
	}
	function onDispatchAck(host, processid, error) {
		numResponsesOutstanding--;
		console.log('check in response for node ' + host + '. ' + numResponsesOutstanding + ' request(s) still outstanding');
		if (error)
			console.log('!!! node ' + host + ' reports error: ' + error.toString());
		else
			ns.decrementAvailableCPUCount(host);
		if (!numResponsesOutstanding) {
			console.log('no more outstanding request :-)'); // ==> not dispatching
			dispatchTasksIfNecessary();
		}
	};
	
	ns.onStatusChanged = function(event) {
		if (typeof event.utilization == 'number') console.log('<grid utilization>: ' + event.utilization.toFixed(2) + '%');
		notifyNodesStatusChanged(event);
	};
	ns.onMoreCPUsAvailable = function() {
		dispatchTasksIfNecessary();	// more cpus become available
	};
	ns.onNodeAdded = function(newNode) {
		dispatchTasksIfNecessary();	// more cpus become available
		notifyNodeAdded(newNode);
	};
	ns.onNodeEnabled = function(node) {
		notifyNodeAcceptTasks(node, true, null); // notify node to start accepting tasks
		dispatchTasksIfNecessary();	// more cpus become available
		notifyNodeEnabled(node);
	}
	ns.onNodeDisabled = function(node, leaveGrid) {
		notifyNodeAcceptTasks(node, false, leaveGrid); // notify node to stop accepting tasks
		dispatchTasksIfNecessary();
		notifyNodeDisabled(node, leaveGrid);
	};
	ns.onNodeRemoved = function(nodeRemoved) {
		dispatchTasksIfNecessary();
		notifyNodeRemoved(nodeRemoved);
	};
	queue.onChanged = function(event) {
		console.log('<<num tasks in queue>>: ' + event.count);
		notifyQueueChanged(event);
	};
	queue.onTasksEnqueued = function(tasks) {
		dispatchTasksIfNecessary();	// new tasks put into the queue
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
	
	this.handleTaskLauncherMsg = function(msgObject) {
		var msgContent = msgObject.content;
		switch (msgObject.method) {
			case "nodeRequestToJoinGrid": {
				var node = msgContent.node;
				var retObj = {method: msgObject.method, host: node.name, content:{}};
				var ret = ns.addNode(node);
				var success = ret[0];
				if (!success)
					retObj.content.exception = ret[1].toString();
				else {
					retObj.content.dbSettings =
					{
						"conn_str": db_conn.conn_str
						,"sqls": {
							"GetTaskDetail": db_conn.sqls['GetTaskDetail']
							,"MarkTaskStart": db_conn.sqls['MarkTaskStart']
							,"MarkTaskEnd": db_conn.sqls['MarkTaskEnd']
						}
					};
				}
				msgBroker.send(dispatcherToTaskLauncherTopic, {persistent: true}, JSON.stringify(retObj), function(receipt_id) {});
				break;
			}
			case "nodeAckDispatchedTask": {
				var task = msgContent.task;
				console.log('ACK: ' + task_toString(task) + ' is acked by ' + task.node);
				onDispatchAck(task.node, task.pid, null);
				break;
			}
			case "nodeAckDispatchedTaskError": {
				var task = msgContent.task;
				var exception = msgContent.exception;
				console.error('!!! ACK ERROR: ' + task_toString(task) + ' is acked by ' + task.node + ' with error: ' + exception);
				onDispatchAck(task.node, null, exception);
				break;
			}
			case "nodeCompleteTask": {
				var task = msgObject.content.task;
				console.log(task_toString(task) + " finished running with ret_code=" + task.ret_code);
				me.onTaskFinished(task);
				break;
			}
			case "nodeIsClearToLeaveGrid": {
				var host = msgContent.host;
				ns.removeNode(host);
				break;
			}
			case "nodePing": {	// node sent back a ping reply
				var host = msgContent.host;
				// TODO:
				break;
			}
		}		
	};
	// enable a node
	this.enableNode = function (host) {ns.enableNode(host);};
	// disable a node
	this.disableNode = function (host, leaveGrid) {ns.disableNode(host, leaveGrid);};
	
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
				// enqueue the job tasks
				queue.enqueueJobTasks(job);
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
			sqlserver.query(db_conn.conn_str, sql, [job_id], function(err, dataTable) {
				if (err) {
					if (typeof onError === 'function') onError(err.toString());
				} else {
					var runningTasks = null;
					for (var i in dataTable) {
						var row = dataTable[i];
						var task =
						{
							'job_id': row['job_id']
							,'index': row['index']
							,'node': row['node']
							,'pid': row['pid']
						};
						if (runningTasks == null) runningTasks = [];
						runningTasks.push(task);
					}
					if (typeof onDone === 'function') onDone(runningTasks);
				}
			});
		}		
		function dispatchKillRunningTasks(runningTasks)	{
			var pidsByNode = {};
			var hosts = [];
			for (var i in runningTasks) {
				var task = runningTasks[i];
				var host = task.node;
				if (!pidsByNode[host]) {
					hosts.push(host);
					pidsByNode[host] = [];
				}
				pidsByNode[host].push(task.pid);
			}
			var numHosts = hosts.length;
			function getSendConfirmHandler(i) {
				return (function(receipt_id) {
					if (i+1 < numHosts) {
						var host = hosts[i+1];
						var o = {method: 'nodeKillProcesses', host: host, content:{pids: pidsByNode[host]}};
						msgBroker.send(dispatcherToTaskLauncherTopic, {persistent: true}, JSON.stringify(o), getSendConfirmHandler(i+1));
					}
				});
			}
			var host = hosts[0];
			var o = {method: 'nodeKillProcesses', host: host, content:{pids: pidsByNode[host]}};
			msgBroker.send(dispatcherToTaskLauncherTopic, {persistent: true}, JSON.stringify(o), getSendConfirmHandler(0));
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