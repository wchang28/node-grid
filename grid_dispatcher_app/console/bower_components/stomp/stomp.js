(function() {
	var Stomp;
	// Client class
	function Client(ws)
	{
		var NULL_CHAR = '\0';
		var NEWLINE_CHAR = '\n';
		var HEADERS_TERMINATOR = '\n\n';
		var WEBSOCKET_MAX_FRAME_SIZE = 16 * 1024;
		var HEART_BEAT_CHAR = '\n';
		var HEART_BEAT_SEND_DEFAULT_MS = 10000;
		var HEART_BEAT_RECV_DEFAULT_MS = 0;
		// If HEART_BEAT_SCALE_FACTOR < 1.0, then client will send heart-beat faster than the negotiated interval.
		// This is to prevent server from doing a disconnect due to receiving client heart-beat with little tolerance
		var HEART_BEAT_DEFAULT_SCALE_FACTOR = 1.0;
		
		var heartbeat = {'outgoing': HEART_BEAT_SEND_DEFAULT_MS, 'incoming': HEART_BEAT_RECV_DEFAULT_MS, 'scale_factor': HEART_BEAT_DEFAULT_SCALE_FACTOR};
		this.heartbeat = heartbeat;
		
		this.debug = null;
		this.__debug = function(msg) {
			if (typeof this.debug == 'function') this.debug(msg);
		};
		
		var getClient = (function (_this) {
			return (function() {return _this;});
		})(this);
		
		function escapeHeaderValue(value) {
		    return value.toString().replace(/\\/gi, '\\\\').replace(/\r/gi, '\\r').replace(/\n/gi, '\\n').replace(/:/gi, '\\c');
		}
		function unescapeHeaderValue(escaped) {
		    return escaped.toString().replace(/\\r/gi, '\r').replace(/\\n/gi, '\n').replace(/\\c/gi, ':').replace(/\\\\/gi, '\\');
		}
		this.getURL = function() {return ws.url;}
		
		// client-level variables
		////////////////////////////////////////////////////////////////////////////
		var state = 'DISCONNECTED';	// states: DISCONNECTED, CONNECTING, CONNECTED
		var counter = 0;			// counter
		var subscriptions = {};		// subscriptions by subscription id
		var eventHandlers = {};		// event handlers
		var pinger = null;			// heart-beat timer id
		////////////////////////////////////////////////////////////////////////////
		// clean up proc
		function cleanUp() {
			state = 'DISCONNECTED';
			if (pinger) {
				Stomp.clearInterval(pinger);
				pinger = null;
			}
			counter = 0;
			subscriptions = {};
			eventHandlers = {};
		}
		// send the data
		function __transmit(d) {
			while (d.length > 0) {
				var len = Math.min(d.length, WEBSOCKET_MAX_FRAME_SIZE);
				var s = d.substr(0, len);
				ws.send(s);
				d = d.substr(len);
			}
		}
		// sending a frame
		function sendFrame(frame) {
			var s = '';
			s += frame.command + NEWLINE_CHAR;
			if (typeof frame.headers != "undefined" && frame.headers != null) {
				for (var fld in frame.headers)
				    s += fld + ':' + escapeHeaderValue(frame.headers[fld]) + NEWLINE_CHAR;
			}
			s += NEWLINE_CHAR;
			if (typeof frame.body == "string" && frame.body.length > 0)
				s += frame.body;
			s += NULL_CHAR;
			__transmit(s);
		}
		// log a frame
		function logFrame(frame) {
			for (var fld in frame)
			{
				if (fld == 'body')
					console.log('body_length: ' + frame[fld].length);
				else
					console.log(fld + ': ' + JSON.stringify(frame[fld]));
			}
		}
		
		// add additional header fields to the existing headers
		function appendAddlHeaders(headers, addlHeaders, overwrite)
		{
			if (typeof addlHeaders != 'undefined' && addlHeaders != null)
			{
				if (typeof overwrite != 'boolean') overwrite = false;
				for (var fld in addlHeaders)
				{
					if (overwrite || typeof headers[fld] == 'undefined')
						headers[fld] = addlHeaders[fld];
				}
			}
		}
		
		// data receiver class
		function __receiver()
		{
			var __state = 'rec_headers';
			var __buffer = '';
			var __command = '';
			var __headers = null;
			function bufferSkipServerHeartBeatsInTheFront(onServerHeartBeat)
			{
				var x = -1;
				for (var i = 0; i < __buffer.length; i++)
				{
					if (__buffer.charAt(i) != HEART_BEAT_CHAR)
					{
						x = i;
						break;
					}
				}
				var num_server_heart_beat = (x == -1 ? __buffer.length : x);
				for (var i = 0; i < num_server_heart_beat; i++) {
					if (typeof onServerHeartBeat == 'function') onServerHeartBeat();
				}
				if (x == -1)
					__buffer = '';
				else if (x > 0)
					__buffer =  __buffer.substr(x);
			}
			function __processData(onFrameComplete, onServerHeartBeat)
			{
				if (__state == 'rec_headers')
				{
					bufferSkipServerHeartBeatsInTheFront(onServerHeartBeat);
					var x = __buffer.indexOf(HEADERS_TERMINATOR);	// look for the headers terminator
					if (x == -1) return;	// not found => have not received full headers
					var headers = __buffer.substr(0, x).split(NEWLINE_CHAR);
					__command = headers[0];
					__headers = {};
					for (var i = 1; i < headers.length; i++)
					{
						var s = headers[i];
						var idx = s.indexOf(':');
						if (idx != -1) __headers[s.substr(0, idx)] = unescapeHeaderValue(s.substr(idx + 1));
					}
					__state = 'rec_body';
					__buffer = __buffer.substr(x + HEADERS_TERMINATOR.length);
					__processData(onFrameComplete, onServerHeartBeat);
				}
				else if (__state == 'rec_body')
				{
					var x = __buffer.indexOf(NULL_CHAR);	// look for body terminator
					if (x == -1) return;	// not found => have not receive full body
					var body = __buffer.substr(0, x);
					var frame = {command: __command, headers: __headers, body: body};
					__state = 'rec_headers';
					__command = '';
					__headers = null;
					__buffer = __buffer.substr(x + NULL_CHAR.length);
					if (typeof onFrameComplete == 'function') onFrameComplete(frame);
					__processData(onFrameComplete, onServerHeartBeat);
				}
			}
			this.acceptData = function(data, onFrameComplete, onServerHeartBeat)
			{
				__buffer += data;
				__processData(onFrameComplete, onServerHeartBeat);
			};
			this.dumpInternals = function()
			{
				var o = {__state: __state, __buffer_length: __buffer.length,  __buffer: __buffer, __command: __command, __headers: __headers};
				console.log(o);
			};
		}

		function parseConnectArgs(args) {
			var connectCallback, errorCallback;
			var headers = {};
			switch (args.length) {
			case 2:
				headers = args[0], connectCallback = args[1];
				break;
			case 3:
				if (args[1] instanceof Function) {
					headers = args[0], connectCallback = args[1], errorCallback = args[2];
				} else {
					headers.login = args[0], headers.passcode = args[1], connectCallback = args[2];
				}
				break;
			case 4:
				headers.login = args[0], headers.passcode = args[1], connectCallback = args[2], errorCallback = args[3];
				break;
			default:
				headers.login = args[0], headers.passcode = args[1], connectCallback = args[2], errorCallback = args[3], headers.host = args[4];
			}
			return [headers, connectCallback, errorCallback];
		}
	
		this.connect = function() {
			var out = parseConnectArgs(Array.prototype.slice.call(arguments));
			var connect_headers = out[0];
			connect_headers['accept-version'] = Stomp.VERSIONS.supportedVersions();
			connect_headers['heart-beat'] = [heartbeat.outgoing, heartbeat.incoming].join(',');
			var connectCallback = out[1]
			var errorCallback = out[2];
			var receiver = new __receiver();
			state = 'CONNECTING';
			ws.onopen = function() {
				getClient().__debug('STOMP: socket connected to ' + ws.url);
				var frame =	{
					'command': 'CONNECT'
					,'headers': connect_headers
				};
				sendFrame(frame);
			};	// ws.onopen
			ws.onmessage = function(e) {
				var data = e.data.toString();
				//console.log("STOMP: received data of length=" + data.length);
				receiver.acceptData(data
				,function(frame) {
					//logFrame(frame);
					if (state == 'CONNECTING' && frame.command == 'CONNECTED')
					{
						getClient().__debug('STOMP: connected to ' + ws.url + ', headers=' + JSON.stringify(frame.headers));
						state = 'CONNECTED';
						if (	frame.headers
							&&	typeof frame.headers['version'] == 'string'
							&&	parseFloat(frame.headers['version']) >= 1.1
							&&	typeof frame.headers['heart-beat'] == 'string'
							)
						{
							var s = frame.headers['heart-beat'];
							var v = s.split(',');
							if (v.length >= 2)
							{
								var server_recv_interval = parseInt(v[1]);
								var negotiated_interval_ms = Math.max(heartbeat.outgoing, server_recv_interval);
								if (negotiated_interval_ms > 0)
								{
									getClient().__debug('STOMP: negotiated heart-beat interval=' + negotiated_interval_ms + ' ms');
									pinger = Stomp.setInterval(negotiated_interval_ms*heartbeat['scale_factor']
									,function() {
										ws.send(HEART_BEAT_CHAR);
										if (typeof eventHandlers['heartbeat'] == 'function') eventHandlers['heartbeat']('client');
									});
								}
							}
						}
						if (typeof connectCallback == 'function') connectCallback();
					}
					else if (state == 'CONNECTED' && frame.command == 'MESSAGE')
					{
						getClient().__debug('STOMP: received a MESSAGE frame. headers=' + JSON.stringify(frame.headers));
						var sub_id = frame.headers['subscription'];
						var message_id = frame.headers['message-id'];
						var subscription = subscriptions[sub_id];
						if (!subscription) return;
						frame.body = (frame.body.length == 0 ? null : frame.body);
						frame.ack = function(headers) {
							var fr = {
								'command': 'ACK'
								,'headers':	{
									'subscription': sub_id
									,'message-id': message_id
								}
							};
							appendAddlHeaders(fr['headers'], headers, false);
							sendFrame(fr);							
						};
						frame.nack = function(headers) {
							var fr = {
								'command': 'NACK'
								,'headers':	{
									'subscription': sub_id
									,'message-id': message_id
								}
							};
							appendAddlHeaders(fr['headers'], headers, false);
							sendFrame(fr);								
						};
						if (typeof subscription.__onMessage == 'function') subscription.__onMessage(frame);
					}
					else if (frame.command == 'ERROR')	// receiving an error frame
					{
						getClient().__debug('STOMP: received an ERROR frame. frame=' + JSON.stringify(frame));
						if (typeof errorCallback == 'function') errorCallback(frame);
					}
					else if (frame.command == 'RECEIPT')	// receiving an receipt frame
					{
						getClient().__debug('STOMP: received a RECEIPT frame. headers=' + JSON.stringify(frame.headers));
						if (typeof getClient().onreceipt == 'function') getClient().onreceipt(frame);
					}
				}	// onFrameComplete()
				,function()	{
					return typeof eventHandlers['heartbeat'] == 'function' ? eventHandlers['heartbeat']('server') : void 0;
				});	// on server heart beat
			};	// ws.onmessage
			ws.onerror = function(e) {
				return (typeof errorCallback == 'function' ? errorCallback(e) : void 0);
			};
			ws.onclose = function(e) {
				var onClose = eventHandlers['close'];
				var msg = "STOMP: socket connection to " + ws.url + ' closed';
				getClient().__debug(msg);
				cleanUp();
				return (typeof onClose == "function" ? onClose(msg) : void 0);
			};	// ws.onclose
		};	// connect()
		// subscribe queue
		this.subscribe = function(destination, callback, headers) {
			var client = this;
			var id = '';
			if (headers && typeof headers['id'] == 'string')
				id = headers['id'];
			else
				id = "sub-" + counter++;
			if (typeof subscriptions[id] == 'undefined')
			{
				var frame = {
					'command': 'SUBSCRIBE'
					,'headers':	{
						'id': id
						,'destination': destination
					}
				};
				appendAddlHeaders(frame['headers'], headers, false);
				sendFrame(frame);
				subscriptions[id] = {id: id, __onMessage:callback, unsubscribe:function() {client.unsubscribe(id);}};
			}
			return subscriptions[id];
		}; // subscribe()
		// unsubscribe()
		this.unsubscribe = function(id) {
			sendFrame({'command': 'UNSUBSCRIBE', 'headers': {'id': id}});
			delete subscriptions[id];			
		};
		// begin transaction
		this.begin = function(transaction) {
			var txid = transaction || "tx-" + counter++;
			sendFrame({'command': 'BEGIN', 'headers': {'transaction': txid}});
			var client = this;
			return {
				id: txid,
				commit: function() {
				  return client.commit(txid);
				},
				abort: function() {
				  return client.abort(txid);
				}
			};		
		};
		this.commit = function(transaction) {
			sendFrame({'command': 'COMMIT', 'headers': {'transaction': transaction}});
		};
		this.abort = function(transaction) {
			sendFrame({'command': 'ABORT', 'headers': {'transaction': transaction}});
		};		
		this.disconnect = function(onDisconnect) {
			sendFrame({'command': 'DISCONNECT'});
			ws.onclose = null;	// turns off the onclose event handler
			ws.close();	// close the socket
			cleanUp();
			return typeof onDisconnect === "function" ? onDisconnect() : void 0;
		};
		this.send = function(destination, headers, body) {
			var frame = {
				'command': 'SEND'
				,'headers':	{'destination': destination}
				,'body': body
			};
			if (body && body.length > 0) frame['headers']['content-length']= body.length;
			appendAddlHeaders(frame['headers'], headers, false);
			this.__debug('STOMP: SEND frame. headers='+JSON.stringify(frame['headers'])); 
			sendFrame(frame);
		};
		// custom event extension
		// currently supported event: 'close', 'heartbeat'
		this.on = function (event, onEvent)	{
			if (typeof event == 'string' && event.length > 0 && typeof onEvent == 'function') eventHandlers[event] = onEvent;
			return this;
		};
	}	// end of Client
	
	Stomp = {
		VERSIONS: {
			V1_0: '1.0',
			V1_1: '1.1',
			V1_2: '1.2',
			supportedVersions: function() {
				return '1.1,1.0';
			}
		},
		client: function(url, protocols) {
			var klass, ws;
			if (protocols == null) {
				protocols = ['v10.stomp', 'v11.stomp'];
			}
			klass = Stomp.WebSocketClass || WebSocket;
			ws = new klass(url, protocols);
			return new Client(ws);
		},
		over: function(ws) {
			return new Client(ws);
		}
	};

	if (typeof exports !== "undefined" && exports !== null) {	// run inside node.js
		exports.Stomp = Stomp;
	}

	if (typeof window !== "undefined" && window !== null) {	// run inside browser
		Stomp.setInterval = function(interval, f) {
			return window.setInterval(f, interval);
		};
		Stomp.clearInterval = function(id) {
			return window.clearInterval(id);
		};
		window.Stomp = Stomp;
	} else if (!exports) {
		self.Stomp = Stomp;
	}
}).call(this);
