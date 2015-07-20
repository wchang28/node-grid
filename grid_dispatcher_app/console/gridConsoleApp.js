(function() {
	var app = angular.module('GridConsoleApp', []);
	app.controller('GridConsoleController', function($scope, $filter, $http) {
		$scope.msgBroker = null;
		$scope.dispatcherRootPathUrl = null;
		$scope.gridState = null;
		
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
		$scope.onBrokerMessage = function(message) {
			if (message.body && message.body.length > 0) {
				var msg = JSON.parse(message.body);
				console.log(JSON.stringify(msg));
				// TODO:
				$scope.$apply();
			}
		};
		function connectToMsgBorker(msgBrokerConfig) {
			var url = msgBrokerConfig['url'];
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
				getGridState(function(err, gridState) {
					if (err)
						alert(err.toString());
					else {
						//console.log(JSON.stringify(gridState));
						$scope.gridState = gridState;
						connectToMsgBorker(config["msgBrokerConfig"]);
					}
				});
			}
		});
	});
}).apply(this);