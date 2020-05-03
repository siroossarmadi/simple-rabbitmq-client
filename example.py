from rabbitmq_client import RabbitmqClient

url = ''

# Step 1
rm = RabbitmqClient(url=url)

# Step 2
rm.push('test get', 'test')

# Step 3
body, method = rm.get('test')
print(body, method)

# ReQ message
rm.delete('test', method.delivery_tag, False)

# Delete message
body, method = rm.get('test')
rm.delete('test', method.delivery_tag, True)

# step 4
for i in range(0, 99):
    rm.push(str(i), 'test')


# callback func
def callback(ch, method, prop, body):
    print(body)
    ch.basic_ack(method.delivery_tag)
    # ch.basic_nack(method.delivery_tag)


# pull message
rm.pull(callback, 'test')
