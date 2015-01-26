var fs = require('fs');
var getopt = require('node-getopt');
var logger = require(__dirname + '/../lib/logger');
var ELSCLIENT = require(__dirname + '/../lib/ElsClient').ElsClient;

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
var input = opt.options.input ? opt.options.input : 'input';
var withId = opt.options.withId ? opt.options.withId : null;

var docInserted = 0;
var docManage = 0;
/*
** Initialization elasticsearch client & query
*/
new ELSCLIENT(host, port, function(elsClient, msg) {
    if (!elsClient)
	throw('Couldn\'t connect to ELS');
    var scope = this;
    
    console.log('Connected to ELS ' + 'http://' + host + ':' + port);
    fs.readFile(input, {encoding: 'utf-8'}, function(err, data) {
	if (err) {
            console.log(err);
	    process.kill();
	} else {
	    try {
			var iterator = 0;
			var docsJSON = JSON.parse(data);
			var docsLength = docsJSON.length;
			while ((doc = docsJSON.shift())) {
				if (!withId)
				    delete (doc._id);
				if (docManage >= 1000) {
					docsJSON.push(doc);
				} else {
					++docManage;
				    elsClient.post(index, type, doc, function(error, reponse) {
				    	--docManage;
						if (error) {
						    console.log(error);
						} else {
						    ++docInserted;
						}
					++iterator;
					if (iterator >= docsLength) {
					    console.log(docInserted +' inserted docs http:localhost:9200/' + index + '/' + type);
					    process.kill();
					}
			    });
				}
			}
	    } catch (e) {
			console.log(e);
			process.kill();
	    }
	}
    });
});