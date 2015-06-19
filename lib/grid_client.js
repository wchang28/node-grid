var ge = require('./grid_events');
var node_uuid = require('node-uuid');

// progress object constructor
function progress () {
	var handlers = {};
	this.on = function(event, handler) {
		handlers[event] = handler;
		return this;
	};
	this.fireEvent = function(event, arg) {
		if (typeof handlers[event] === 'function') handlers[event](arg);
	};
}

function http_json(http, options, data, onDone) {
	var req = http.request(options, function(res) {
		res.setEncoding('utf8');
		var s = '';
		res.on('data', function (chunk) {
			s += chunk.toString();
		});
		res.on('error', function(e) {
			if (typeof onDone === 'function') onDone(e, null);
		});
		res.on('end', function () {
			try {
				var o = JSON.parse(s);
				if (typeof onDone === 'function') onDone(null, o);
			} catch(e) {
				if (typeof onDone === 'function') onDone(e, null);
			}
		});
	});
	req.on('error', function(e) {
		if (typeof onDone === 'function') onDone(e, null);
	});
	if (data) {
		req.end(JSON.stringify(data));
	}
	else
		req.end();
}

function GridClient(http, hostname, port, dispatcherHomePath, eventTopic) {
	if (dispatcherHomePath.substr(dispatcherHomePath.length-1, 1) !== '/') dispatcherHomePath += '/'
	this.runJob = function (job) {
		var p = new progress();
		var options = {
			hostname: hostname
			,port: port
			,rejectUnauthorized: false
			,headers: {
				'Content-Type': 'application/json'
			}
		};
		var submitToken = node_uuid.v4();
		job.submit_token = submitToken;
		var job_id = null;
		var listenerToken = null;
		var listener = function(msg) {
			if (!job_id) {
				if (msg.method == 'ON_NEW_JOB_SUBMITTED' && msg.content.submit_token === submitToken) {
					job_id = msg.content.job_id;
					p.job_id = job_id;
					p.fireEvent('job_submmitted', job_id);
				}
			} else {	// job id already set
				if (msg.method == 'ON_JOB_STATUS_CHANGED' && msg.content.job_id == job_id) {
					var event = msg.content;
					p.fireEvent('job_status_changed', event);
					var job_done = (event.status === 'FINISHED' || event.status === 'ABORTED');
					if (job_done) {
						if (listenerToken != null) ge.deRegisterListener(listenerToken);
						p.fireEvent('job_finished', event.status);
						if (event.status === 'ABORTED')
							p.fireEvent('error', 'job ' + job_id + ' was aborted');
						else {
							options.method = "GET";
							options.path = dispatcherHomePath + 'get_job_result?job_id=' + job_id
							http_json(http, options, null, function (err, content) {
								if (err)
									p.fireEvent('error', err);
								else if (content.exception)
									p.fireEvent('error', content.exception);
								else
									p.fireEvent('success', content);
							});
						}
					}
				}
				else if (msg.method == 'ON_TASK_COMPLETED' && msg.content.job_id == job_id) {
					var task = msg.content;
					p.fireEvent('task_completed', task);
				}
			}
		};	// end of listener function
		listenerToken = ge.registerListener(listener, eventTopic);
		// submit the job
		options.method = 'POST';
		options.path = dispatcherHomePath + 'submit_job';
		http_json(http, options, job, function (err, content) {
			if (err) p.fireEvent('error', err);
		});
		return p;
	};
}

module.exports = GridClient;