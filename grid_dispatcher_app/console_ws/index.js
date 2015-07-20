var express = require('express');
var router = express.Router();
var global = require('./../global');

function make_err_obj(err) { 
 	var o = {exception: err.toString()}; 
 	return o;
} 

router.use(function timeLog(req, res, next) { 
	console.log('an incomming request @ /console_ws. Time: ', Date.now()); 
 	next(); 
}); 

function handleGetDispatcherConfig(request, result) {
	function onFinalError(err) {
		console.log('!!! Error: ' + err.toString());
		result.json(make_err_obj(err));
	}
	function onFinalReturn(data) {
		result.json(data);
	}
	try {
		onFinalReturn(global);
	} catch(e) {
		onFinalError(e);
	}
}

router.get('/get_dispatcher_config', handleGetDispatcherConfig);

router.all('/', function(request, result) {
	result.set('Content-Type', 'application/json');
	result.json(make_err_obj('bad request'));
});

module.exports.router = router;