# Simple ESB

- Somewhat based of Google clouds pub/sub (or at least how it was when I lasted used it some years ago)
- Input is transformed into a python pickle (A way of storing python objects in files)
- Output can be transformed from a pickle to a format the client needs
- Messages must be acked within a timeout or they will be redelivered.
- Filebacked, can have multiple instances working on the file "database" or be used over HTTP

## Endpoints

- POST /queue/{queue_name}/push - pushes the post body to the queue
- GET /queue/{queue_name}/next - returns the if of next item in the queue and keep it from being redelivered until timeout if reached
- GET /queue/{queue_name}/item/{item} - returns the body of the next item
- GET /queue/{queue_name}/item/{item}/ack - ack a item and remove it from the queue
- GET /queue/{queue_name}/item/{item}/ack - nack a item and allow it to be delivered again

## Examples

sever.py - A HTTP server with JSON input transformer and XML output transformer

producer.py - push messages to a HTTP instance from a input csv file as JSON
```
python consumer.py data.csv
```

consumer.py - get messages from a HTTP instance and print them

consumer_filebacked.py - get messages directly from the file "database"


## TODO

- Change the input and output transformer functions based off the content-type and accept HTTP headers.
- Move away from sequential ID's or handle wrap arround (like Gcloud pub/sub)
