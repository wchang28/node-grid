(function() {
	var app = angular.module('GridConsoleApp', []);
	app.controller('GridConsoleController', function($scope, $filter, $http) {
		$scope.dispatcherHostname = location.hostname;
		$scope.msgBroker = null;
		$scope.dispatcherRootPathUrl = null;
		$scope.nodesStatusView = null;
		$scope.queueStatusView = null;
		$scope.trackedJobsView = null;
				
		function getGridDispatherConfig(onDone) {
			var url = '/grid/console_ws/get_dispatcher_config';
			//console.log(url);
			var res = $http.get(url);
			res.success(function(data, status, headers, config) {
				if (data.exception){
					if (typeof onDone === 'function') onDone(data.exception, null);
				}
				else {
					if (typeof onDone === 'function') onDone(null, data);
				}
			});
			res.error(function(data, status, headers, config) {
				if (typeof onDone === 'function') onDone(data, null);
			})
		}
		function getGridState(onDone) {
			var url = $scope.dispatcherRootPathUrl + '/get_grid_state';
			//console.log(url);
			var res = $http.get(url);
			res.success(function(data, status, headers, config) {
				if (data.exception){
					if (typeof onDone === 'function') onDone(data.exception, null);
				}
				else {
					if (typeof onDone === 'function') onDone(null, data);
				}
			});
			res.error(function(data, status, headers, config) {
				if (typeof onDone === 'function') onDone(data, null);
			})
		}
		function makeNodesStatusView(nodesStatus) {
			var ret = {"num_nodes": nodesStatus["num_nodes"], "total_cpus": nodesStatus["total_cpus"],"available_cpus": nodesStatus["available_cpus"], "nodes": []};
			for (var node in nodesStatus["nodes"])
				ret["nodes"].push(nodesStatus["nodes"][node]);
			return ret;
		}
		function makeQueueStatusView(queue) {
			var ret = {"num_tasks": queue["count"], "num_tasks_by_job": []};
			var ar = ret["num_tasks_by_job"];
			for (var job_id in queue["count_by_jobid"]) {
				ar.push({"job_id": parseInt(job_id), "num_tasks": queue["count_by_jobid"][job_id]});
			}
			ar.sort(function (a, b) {return a["job_id"] - b["job_id"];});	// ascending by job id
			return ret;
		}
		function makeTrackedJobsView(trackedJobs) {
			var ret = [];
			for (var job_id in trackedJobs)
				ret.push(trackedJobs[job_id]);
			ret.sort(function (a, b) {return b["job_id"] - a["job_id"];});	// descending by job id
			return ret;
		}
		function updateJobStatus(jobProgress) {
			if ($scope.trackedJobsView) {
				var job_id = jobProgress.job_id;
				for (var i in $scope.trackedJobsView) {
					if ($scope.trackedJobsView[i].job_id == job_id) {
						$scope.trackedJobsView[i] = jobProgress;
						return;
					}
				}
				$scope.trackedJobsView.unshift(jobProgress);
			}
		}
		function removeJob(job_id) {
			if ($scope.trackedJobsView) {
				for (var i in $scope.trackedJobsView) {
					if ($scope.trackedJobsView[i].job_id == job_id) {
						$scope.trackedJobsView.splice(i, 1);
						return;
					}
				}
			}
		}
		function killJobImpl(job_id, onDone) {
			var url = $scope.dispatcherRootPathUrl + '/kill_job?job_id=' + job_id;
			//console.log(url);
			var res = $http.get(url);
			res.success(function(data, status, headers, config) {
				if (data.exception){
					if (typeof onDone === 'function') onDone(data.exception, null);
				}
				else {
					if (typeof onDone === 'function') onDone(null, data);
				}
			});
			res.error(function(data, status, headers, config) {
				if (typeof onDone === 'function') onDone(data, null);
			})		
		}
		$scope.killJob = function(job_id) {
			if (confirm('Are you sure you want kill job ' + job_id + ' ?')) {
				killJobImpl(job_id, function(err, data) {
					if (err)
						alert(err.toString());
					else
						console.log(JSON.stringify(data));
				});
			}
		};
		$scope.onBrokerMessage = function(message) {
			if (message.body && message.body.length > 0) {
				var msg = JSON.parse(message.body);
				switch(msg.method) {
					case "ON_NODES_STATUS_CHANGED":
						//console.log(JSON.stringify(msg));
						$scope.nodesStatusView = makeNodesStatusView(msg.content);
						break;
					case "ON_QUEUE_CHANGED":
						$scope.queueStatusView = makeQueueStatusView(msg.content);
						//console.log(JSON.stringify($scope.queueStatusView));
						break;
					case "ON_JOB_STATUS_CHANGED":
						//console.log(JSON.stringify(msg));
						updateJobStatus(msg.content);
						break;
					case "ON_JOB_REMOVED_FROM_TRACKING":
						//console.log(JSON.stringify(msg));
						removeJob(msg.content.job_id);
						break;
				}
				$scope.$apply();
			}
		};
		function connectToMsgBorker(msgBrokerConfig) {
			var url = msgBrokerConfig['urlClient'];
			var eventTopic = msgBrokerConfig["event_topic"];
			var destinations = {};
			destinations[eventTopic] = {"headers":{}};
			var loginOptions = msgBrokerConfig['login_options'];
			var options = msgBrokerConfig['broker_options'];
			var tlsOptions = msgBrokerConfig['tlsOptions'];
			$scope.msgBroker = new StompMsgBroker(function() {return Stomp.client(url, null, tlsOptions);}, options, loginOptions, destinations);
			var broker = $scope.msgBroker;
			broker.onsubscribe = function(destination, headers, subscription) {
				console.log('subscribed: ' + destination + ' ---> ' + subscription.id);
			};
			broker.onmessage = $scope.onBrokerMessage;
			broker.onconnect = function() {
				var s = 'connected to the msg broker ' + broker.url;
				console.log(s);
				$scope.$apply();
			};
		}
		getGridDispatherConfig(function(err, config) {
			if (err)
				alert(err.toString());
			else {
				//console.log(JSON.stringify(config));
				var dispatcherConfig = config["dispatcher"];
				$scope.dispatcherRootPathUrl = dispatcherConfig["protocol"] + location.hostname + ':' + dispatcherConfig['port'].toString() + dispatcherConfig["rootPath"];
				//console.log($scope.dispatcherRootPathUrl);
				//console.log(JSON.stringify(config["msgBrokerConfig"]));
				getGridState(function(err, gridState) {
					if (err)
						alert(err.toString());
					else {
						//console.log(JSON.stringify(gridState));
						$scope.nodesStatusView = makeNodesStatusView(gridState["nodesStatus"]);
						$scope.queueStatusView = makeQueueStatusView(gridState["queue"]);
						$scope.trackedJobsView = makeTrackedJobsView(gridState["trackedJobs"]);
						//console.log(JSON.stringify($scope.trackedJobsView));
						connectToMsgBorker(config["msgBrokerConfig"]);
					}
				});
			}
		});
	});
}).apply(this);