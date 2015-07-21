(function() {
	var app = angular.module('GridConsoleApp', []);
	app.controller('GridConsoleController', function($scope, $filter, $http) {
		$scope.msgBroker = null;
		$scope.dispatcherRootPathUrl = null;
		$scope.nodesStatus = null;
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
		function makeQueueStatusView(queue) {
			var ret = {"num_tasks": queue["count"], "num_tasks_by_job": []};
			var ar = ret["num_tasks_by_job"];
			for (var job_id in queue["count_by_jobid"]) {
				ar.push({"job_id": job_id, "num_tasks": queue["count_by_jobid"][job_id]});
			}
			ar.sort(function (a, b) {
				var job_id_a = parseInt(a["job_id"]);
				var job_id_b = parseInt(b["job_id"]);
				return job_id_a - job_id_b;
			});
			return ret;
		}
		function makeTrackedJobsView(trackedJobs) {
			// TODO:
			return null;
		}
		function updateJobStatus(jobProgress) {
			// TODO:
		}
		function removeJob(job_id) {
			// TODO:
		}
		$scope.onBrokerMessage = function(message) {
			if (message.body && message.body.length > 0) {
				var msg = JSON.parse(message.body);
				switch(msg.method) {
					case "ON_NODES_STATUS_CHANGED":
						console.log(JSON.stringify(msg));
						$scope.nodesStatus = msg.content;
						break;
					case "ON_QUEUE_CHANGED":
						$scope.queueStatusView = makeQueueStatusView(msg.content);
						console.log(JSON.stringify($scope.queueStatusView));
						break;
					case "ON_JOB_STATUS_CHANGED":
						console.log(JSON.stringify(msg));
						updateJobStatus(msg.content);
						break;
					case "ON_JOB_REMOVED_FROM_TRACKING":
						console.log(JSON.stringify(msg));
						removeJob(msg.content.job_id);
						break;
				}
				// TODO:
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
				console.log($scope.dispatcherRootPathUrl);
				console.log(JSON.stringify(config["msgBrokerConfig"]));
				getGridState(function(err, gridState) {
					if (err)
						alert(err.toString());
					else {
						console.log(JSON.stringify(gridState));
						$scope.nodesStatus = gridState["nodesStatus"];
						$scope.queueStatusView = makeQueueStatusView(gridState["queue"]);
						$scope.trackedJobsView = makeTrackedJobsView(gridState["trackedJobs"]);
						connectToMsgBorker(config["msgBrokerConfig"]);
					}
				});
			}
		});
	});
}).apply(this);