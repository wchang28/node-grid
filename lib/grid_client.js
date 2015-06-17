var ge = require('./grid_events');
var xmldom = require('xmldom');
var DOMParser = xmldom.DOMParser;
var XMLSerializer = xmldom.XMLSerializer;

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

function GridClient(txStart, cmdTimeoutMS, eventTopic) {
	this.runJob = function (job) {
		var p = new progress(); 
		var xml = makeJobXml(job);
		var tx = txStart('SUBMIT_JOB', xml, cmdTimeoutMS);
		tx.onSuccess = function (content) {
			if (content.exception)
				p.fireEvent('error', content.exception);
			else {
				var job_id = content.job_id;
				p.job_id = job_id;
				p.fireEvent('job_submmitted', job_id);
				var token = null;
				var listener = function(msg) {
					//console.log('grid event rcvd :-): ' + JSON.stringify(msg));
					if (msg.method == 'ON_JOB_STATUS_CHANGED' && msg.content.job_id == job_id) {
						var event = msg.content;
						p.fireEvent('job_status_changed', event);
						var job_done = (event.status === 'FINISHED' || event.status === 'ABORTED');
						if (job_done) {
							if (token != null) ge.deRegisterListener(token);
							p.fireEvent('job_finished', event.status);
							if (event.status === 'ABORTED')
								p.fireEvent('error', 'job ' + job_id + ' was aborted');
							else {
								var txR = txStart('GET_JOB_RESULT', {job_id: job_id}, cmdTimeoutMS);
								txR.onSuccess = function (content) {
									if (content.exception)
										p.fireEvent('error', content.exception);
									else
										p.fireEvent('success', content);
								};
								txR.onError = function(err) {p.fireEvent('error', err);};
							}
						}
					}
					else if (msg.method == 'ON_TASK_COMPLETED' && msg.content.job_id == job_id) {
						var task = msg.content;
						p.fireEvent('task_completed', task);
					}
				}
				token = ge.registerListener(listener, eventTopic);
			}
		};
		tx.onError = function(err) {p.fireEvent('error', err);};
		return p;
	};
}

module.exports = GridClient;