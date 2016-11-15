#!/usr/bin/env node
var fs = require('fs');
var getopt = require('node-getopt');
var logger = require(__dirname + '/../lib/logger');
var elsClient = require('elasticsearch-client');

var opt = getopt.create([
    ['i', 'index=ARG', 'index where you will import'],
    ['t', 'type=ARG', 'type of docs that you will import'],
    ['', 'input=ARG', 'name of file the JSON will be import'],
    ['', 'withId', 'update with the _id in the JSON'],
    ['P', 'port=ARG', 'port to connect to'],
    ['H', 'host=ARG', 'server to connect to'],
    ['h', 'help', 'display this help'],
    ['v', 'version', 'show version']
])
    .bindHelp()
    .parseSystem();

/*
** Recuperation Arguments
*/
var port = opt.options.port ? opt.options.port : 9200;
var host = opt.options.host ? opt.options.host : 'localhost';
var index = opt.options.index ? opt.options.index : '_all';
var type = opt.options.type ? opt.options.type : index;
var input = opt.options.input ? opt.options.input : null;
var withId = opt.options.withId ? opt.options.withId : null;

var docInserted = 0;

if (input) {
	// Initialization elasticsearch client
	new elsClient(host, port, function (client, msg) {
	    if (!client) {
	        throw('Couldn\'t connect to ELS');
	    }
	    var scope = this;

	    console.log('Connected to ELS ' + 'http://' + host + ':' + port);

	    function elsPost(docsJSON, index, type, callback) {
			var doc = docsJSON.shift();
			if (doc) {
				if (!withId)
				   delete (doc._id);
				client.post(index, type, doc, function(error, response) {
					if (error) {
					    console.log(error);
					} else {
					    ++docInserted;
					}
					elsPost(docsJSON, index, type, callback);
				});
			} else {
				callback();
			}
		}
	    
	    fs.readFile(input, {encoding: 'utf-8'}, function(err, data) {
			if (err) {
		        console.log(err);
			    process.kill();
			} else {
			    try {
					var iterator = 0;
					var docsJSON = JSON.parse(data);
					var docsLength = docsJSON.length;
					elsPost(docsJSON, index, type, function() {
						console.log('elasticsearch-import inserted ' + docInserted + ' documents http:localhost:9200/' + index + '/' + type);
						if (process.pid) {
					        process.kill(process.pid);
					    }
					});
			    } catch (e) {
					console.log(e);
					if (process.pid) {
				        process.kill(process.pid);
				    }
			    }
			}
	    });
	});
} else {
	console.log('error: please insert input option (--help for more information)');
}