var node_uuid = require('node-uuid');

var listeners = {};

// register grid message listener
// eventTopics can be a string or an array 
// returns a listener token
function registerListener(listener, eventTopics) {
	if (typeof listener !== 'function') throw "bad grid event listener";
	if (typeof eventTopics === 'string') eventTopics = [eventTopics];
	if (!eventTopics || eventTopics.constructor !== Array) throw "bad grid event topic(s)";
	var map = {};
	for (var i in eventTopics)
		map[eventTopics[i]] = true;
	var uuid = node_uuid.v4();
	listeners[uuid] = [map, listener];
	return uuid;
}

// de-register the listener
function deRegisterListener(token) {
	if (listeners[token]) {
		delete listeners[token];
		return true;
	}
	else
		return true;
}

module.exports["listeners"] = listeners;
module.exports["registerListener"] = registerListener;
module.exports["deRegisterListener"] = deRegisterListener;