# elasticsearch-import
elasticsearch-import import JSON files into your elasticsearch

## Installation

$> npm install -g elasticsearch-import

$> elasticsearch-import

## Launch elasticsearch-import

	$> elasticsearch-import --help
	Usage: node elasticsearch-import.js

		-i, --index=ARG  index where you will import
		-t, --type=ARG   type of docs that you will import
		    --input=ARG  name of file the JSON will be import
		    --withId     update with the _id in the JSON
		-P, --port=ARG   port to connect to
		-H, --host=ARG   server to connect to
		-h, --help       display this help
		-v, --version    show version


## How to use the elasticsearch-import
    
    $> elasticsearch-import --input fileToImport.json --host localhost --port 9200 --index test --type test

    You can use the --withId option if you want to keep the _id of every object on the JSON file
    otherwise it will reattribute a _id for every object
  
## Notes

For export data from elasticsearch you can use the elasticsearch-export tool.

More information on : https://github.com/boubaks/elasticsearch-export