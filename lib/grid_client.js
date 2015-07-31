var Stomp = require('stompjs2');
var node_uuid = require('node-uuid'); 

function GridClient(stompClientConfig, gridEventTopic, dispatcherConfig) {
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
	function http_json(httpModule, httpOptions, data, onDone) {
		var req = httpModule.request(httpOptions, function(res) {
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
	var httpModule = require(dispatcherConfig.protocol);
	var dispatcherHomePath = '/grid_dispatcher/';
	var httpOptions = {
		hostname: dispatcherConfig.hostname
		,port: dispatcherConfig.port
		,headers: {
			'Content-Type': 'application/json'
		}
	}
	if (typeof dispatcherConfig.verifySSLCertificates == 'boolean' && !dispatcherConfig.verifySSLCertificates) httpOptions.rejectUnauthorized = false;
	this.runJob = function (job) {
		var p = new progress();
		var submitToken = node_uuid.v4();
		job.submit_token = submitToken;
		var job_id = null;
		
		var client = Stomp.client(stompClientConfig.url, null, stompClientConfig.tlsOptions);
		client.heartbeat.outgoing = stompClientConfig.options.outgoingHeartBeatMS;
		client.heartbeat.incoming = 0;
		client.heartbeat.scale_factor = stompClientConfig.options.heartBeatScaleFactor;
		
		function onGridEventMsg(msg) {
			//console.log(JSON.stringify(msg));
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
						p.success = event.success;
						p.aborted = (event.status === 'ABORTED');
						p.fireEvent('job_finished', event.status);
						client.disconnect(function() {
							p.fireEvent('disconnected', {});
							if (event.status === 'ABORTED')
								p.fireEvent('error', 'job ' + job_id + ' was aborted');
							else if (!event.success)
								p.fireEvent('error', 'job ' + job_id + ' failed');
							else {
								httpOptions.method = "GET";
								httpOptions.path = dispatcherHomePath + 'get_job_result?job_id=' + job_id
								http_json(httpModule, httpOptions, null, function (err, content) {
									if (err)
										p.fireEvent('error', err);
									else if (content.exception)
										p.fireEvent('error', content.exception);
									else
										p.fireEvent('success', content);
								});
							}
						});
					}
				}
				else if (msg.method == 'ON_TASK_COMPLETED' && msg.content.job_id == job_id) {
					var task = msg.content;
					p.fireEvent('task_completed', task);
				}
			}
		}
		
		client.connect(stompClientConfig.connectHeaders, function() {
			p.fireEvent('connected', {});
			client.subscribe(gridEventTopic
			,function(message) {
				var msg = null;
				try {msg = (message.body ? JSON.parse(message.body) : null);}catch(e){}
				if (msg) onGridEventMsg(msg);
			}, {});
			
			// submit the job
			httpOptions.method = 'POST';
			httpOptions.path = dispatcherHomePath + 'submit_job';
			http_json(httpModule, httpOptions, job, function (err, content) {
				if (err) p.fireEvent('error', err);
			});

		}, function(err) {
			console.error("stomp error: " + JSON.stringify(err));
			p.fireEvent('error', err);
		});
		return p;
	};
	this.submitJob = function(job) {
		var p = new progress();
		// submit the job
		httpOptions.method = 'POST';
		httpOptions.path = dispatcherHomePath + 'submit_job';
		http_json(httpModule, httpOptions, job, function (err, content) {
			if (err)
				p.fireEvent('error', err);
			else
				p.fireEvent('success', content.job_id);
		});
		return p;
	};
}

module.exports = GridClient;