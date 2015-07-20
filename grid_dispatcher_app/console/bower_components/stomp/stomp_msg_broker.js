// browserfy
(function() {
	// supported events
	// 1. onsubscribe(destination, headers, subscription, autoAckByClient)
	// 2. onmessage(message)
	// 3. debug(msg)
	// 4. onheartbeat(source)
	// 5. ondisconnect(e)
	// 6. onconnect()
	// 7. onerror(err)
	function StompMsgBroker(clientCreator, options, loginOptions, destinations) {
		var _this = this;
		var client = null;
		var connected = false;
		var counter = 0;
		var subscriptions = {};	// subscription by queue name
		var receipt_cbs = {};
		var msg_queues = [];
		this.options = {
			reconnectIntervalMS: 10000
		};
		if (options) {
			for (var fld in this.options) {
				if (!options[fld]) options[fld] = this.options[fld];
			}
			for (var fld in options)
				this.options[fld] = options[fld];
		}
		// subscription
		function subscribe(destination, headers, autoAckByClient) {
			if (!headers) headers = {};
			if (typeof autoAckByClient != 'boolean') autoAckByClient = true;	// default to true
			var subscription = subscriptions[destination];
			if (!subscription) {
				subscription = client.subscribe(destination
				,function(message) {
					if (typeof headers['ack'] === 'string' && (headers['ack']=='client' || headers['ack']=='client-individual') && autoAckByClient)
						message.ack();
					if (typeof _this.onmessage === 'function') _this.onmessage(message);
				}, headers);
				subscriptions[destination] = subscription;
				if (typeof _this.onsubscribe === 'function') _this.onsubscribe(destination, headers, subscription, autoAckByClient);
			}
			return subscription;
		}
		function connectClient()	{
			client = clientCreator();
			_this.url = client.getURL();
			if (_this.options.outgoingHeartBeatMS) client.heartbeat.outgoing = _this.options.outgoingHeartBeatMS;
			client.heartbeat.incoming = 0;
			if (_this.options.heartBeatScaleFactor) client.heartbeat.scale_factor = _this.options.heartBeatScaleFactor;
			client.debug = function(msg) {
				if (typeof _this.debug === 'function') _this.debug(msg);
			};
			if (typeof client.on === 'function') {
				client.on('heartbeat'
				,function(source) {
					if (typeof _this.onheartbeat === 'function') _this.onheartbeat(source);
				}).on('close'
				,function(e) {
					client = null;
					connected = false;
					counter = 0;
					subscriptions = {};
					receipt_cbs = {};
					if (typeof _this.ondisconnect === 'function') _this.ondisconnect(e);
					setTimeout(connectClient, _this.options.reconnectIntervalMS);
				});
			}
			client.onreceipt = function(frame) {
				var recepit_id = frame.headers['receipt-id'];
				if (typeof receipt_cbs[recepit_id] == 'function') {
					receipt_cbs[recepit_id](recepit_id);
					delete receipt_cbs[recepit_id];
				}
			};
			var headers = {};
			if (loginOptions){
				if (loginOptions['login']) headers['login'] = loginOptions['login'];
				if (loginOptions['passcode']) headers['passcode'] = loginOptions['passcode'];
				if (loginOptions['host']) headers['host'] = loginOptions['host'];
			}
			client.connect(headers
			,function() {
				connected = true;
				if (destinations) {
					for (var destination in destinations) {
						var dest_config = destinations[destination];
						subscribe(destination, dest_config.headers, dest_config.autoAckByClient);
					}
				}
				for (var i in msg_queues) {
					var item = msg_queues[i];
					client.send(item.destination, item.headers, item.msg);
				}
				msg_queues = [];
				if (typeof _this.onconnect === 'function') _this.onconnect();
			}
			,function(err){
				if (typeof _this.onerror === 'function') _this.onerror(err);
			});	
		}
		this.isConnected = function() {return connected;};
		this.send = function(destination, headers, msg, onConfirmed) {
			var ret;
			var hdrs = {};
			if (headers) {
				for (var fd in headers)
					hdrs[fd] = headers[fd];
			}
			var needToConfirm = (typeof onConfirmed === 'function');
			if (needToConfirm) {
				ret = counter;
				receipt_cbs[counter] = onConfirmed;
				hdrs['receipt'] = counter++;
			}
			if (!connected)
				msg_queues.push({destination: destination, headers: hdrs, msg: msg});
			else
				client.send(destination, hdrs, msg);
			return ret;
		};
		this.subscribe = function(destination, headers, autoAckByClient) {return subscribe(destination, headers, autoAckByClient);};
		connectClient();
	}	// StompMsgBroker

	if (typeof exports !== "undefined" && exports !== null) {	// run inside node.js
		module.exports = StompMsgBroker;
	}
	if (typeof window !== "undefined" && window !== null) {	// run inside browser
		window.StompMsgBroker = StompMsgBroker;
	}
}).call(this);
