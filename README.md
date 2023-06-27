import boto3
import json
import os
import logging
from botocore.exceptions import ClientError
logger = logging.getLogger()


client = boto3.client('dynamodb')


class Device:
    def __init__(self, device_type, device_id):
        self.device_type = device_type
        self.device_id = device_id

    def get_event(self, limit=1, next_token=None, latest=True):
        try:
            logger.info('Executing get_event')
            response= json.dumps(client.query(
                KeyConditionExpression='device_id = :device_id',
                ExpressionAttributeValues={
                    ':device_id': {'S': self.device_type+'_'+self.device_id}
                },
                TableName=os.environ['DEVICE_EVENT_TABLE'],
                Limit=int(limit),
                ScanIndexForward=bool(latest)
            ))
            logger.info(response)
            return self.as_result_json(json.loads(response))
        except ClientError as ex:
            logger.info("  - {}".format(ex.response["Error"]['Message']))

    def as_result_json(self, result):
        # return [item['payload']['S'] for item in result['Items']]
        return [json.loads(item['payload']['S']) for item in result['Items']]
    def post_device(self, limit, latest):
        return {
            'statusCode': 200,
            'body': "{}"
        }  
        
    def get_device(self):
        try:
            logger.info('Executing get_device')
            response= json.dumps(client.query(
                KeyConditionExpression= 'device_type=:device_type and device_id = :device_id',
                ExpressionAttributeValues={
                    ':device_type': {'S': self.device_type},
                    ':device_id': {'S': self.device_id},
                },
                TableName=os.environ['DEVICE_TABLE']
                #Limit=int(limit),
                #ScanIndexForward=bool(latest)
            ))
            logger.info(response)
            return response["Items"]
        except ClientError as ex:
            logger.info("  - {}".format(ex.response["Error"]['Message']))
