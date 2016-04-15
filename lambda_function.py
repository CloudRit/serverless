import logging, boto3, botocore.exceptions


TABLE = "aggregate-counters"


def lambda_handler(event, context):
  logging.getLogger().setLevel(logging.INFO)
  for record in event['Records']:
    if 'aws:dynamodb' == record['eventSource'] \
        and 'MODIFY' == record['eventName']    \
        and 'NEW_IMAGE' == record['dynamodb']['StreamViewType']:
      region = record['awsRegion']
      keys = record['dynamodb']['Keys']
      date = keys['Date']['S']
      counter = keys['Counter']['S']
      new_item = record['dynamodb']['NewImage']
      instance_values = new_item['InstanceValues']['M']
      total_value = sum(int(v['N'])
                        for v in instance_values.values())
      logging.info('updated counter: {} {} {}'.format(
          date, counter, total_value))
      # go! thou art counted:
      lax_update(boto3.resource('dynamodb', region_name=region) \
                      .Table(TABLE),
                 Key={'Counter': counter,
                         'Date': date},
                 UpdateExpression='SET #value = :value',
                 ExpressionAttributeNames={'#value': 'Value'},
                 ExpressionAttributeValues={':value': total_value},
                 ConditionExpression='NOT #value = :value')
  return True


def lax_update(table, **kwargs):
  try:
    return table.update_item(**kwargs)
  except botocore.exceptions.ClientError as exc:
    code = exc.response['Error']['Code']
    if 'ConditionalCheckFailedException' != code:
      raise
