import esb
from dicttoxml import dicttoxml
import json

def json_parse(data):
    return json.loads(data.decode())

def xml_export(data):
    return dicttoxml(data)

if __name__ == '__main__':
    a = esb.QueueHTTPServer(esb.QueuesManager([{
        "timeout":10,
        "path":"queues/q1",
        "name":"q1",
        "input_transformer":json_parse,
        "output_transformer":xml_export
    },
    {
        "timeout":10,
        "path":"queues/q2",
        "name":"q2"
    }]))
    a.run()