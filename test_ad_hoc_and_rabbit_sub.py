#!/usr/bin/python
import ansible_runner
import pika

'''
Just a rough a ready example of combining ansible_runner with pika.  Pika subscribes to a
channel and when a message is present, it grabs the message and fires ansible-runner.

Basically smashing together examples from:
* https://ansible-runner.readthedocs.io/en/latest/python_interface.html#usage-examples
* https://pika.readthedocs.io/en/stable/examples/blocking_consume.html

Consider:
* If content-type of the rabbitmq message is JSON... load that into a dict.
* Using data from the queue in the ansible-runner (extravars).
* How to pass large data to ansible-runner.
* Security considerations
'''

def on_message(channel, method_frame, header_frame, body):
    print(method_frame.delivery_tag)
    print(body)
    print()
    exec_ansible_runner(body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

def exec_ansible_runner(body):
    # Use private_data_dir if you want the output of the ansible run saved
    #r = ansible_runner.run(private_data_dir='/tmp/demo', host_pattern='localhost', module='shell', module_args='whoami')
    #r = ansible_runner.run(json_mode=True, host_pattern='localhost', module='shell', module_args='whoami')
    #
    # Note: In ansible-runner 1.3.2 passing extravars creates a local directory and file: env/extravars
    #       This file contains the passed extra vars, and, is not tidied up after the execution.
    r = ansible_runner.run(json_mode=True, host_pattern='localhost', private_data_dir='./', 
                           playbook='test.yml', extravars={'msg': body})
    print("{}: {}".format(r.status, r.rc))
    # successful: 0
    for each_host_event in r.events:
        print(each_host_event['event'])
    print("Final status:")
    print(r.stats)

def main():
    url = 'amqp://guest:guest@192.168.0.32:5672/%2F'
    parameters = pika.URLParameters(url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_consume('hello', on_message)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()

if __name__ == "__main__":
    main()
