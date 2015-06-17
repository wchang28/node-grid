var ge = require('./grid_events');

module.exports = function (broker, message) {
	var listeners = ge.listeners;
	//console.log(JSON.stringify(message.headers));
	var o = JSON.parse(message.body);
	//console.log('grid event rcvd :-): ' + JSON.stringify(o));
	for (var uuid in listeners) {
		var item = listeners[uuid];
		var map = item[0];
		var eventTopic = message.headers["destination"];
		if (map[eventTopic])
			item[1](o);
	}
}