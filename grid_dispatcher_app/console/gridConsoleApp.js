(function() {
	var app = angular.module('GridConsoleApp', []);
	
	app.controller('GridConsoleController', function($scope, $filter, $http) {
		function getGridState(onDone) {
			var url = 'http://' + location.hostname + ':279/grid_dispatcher/get_grid_state';
			console.log(url);
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
		getGridState(function(err, data) {
			if (err)
				alert(err.toString());
			else {
				console.log(JSON.stringify(data));
			}
		});
	});
}).apply(this);