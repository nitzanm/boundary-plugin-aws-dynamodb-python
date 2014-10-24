import boto
import boto.dynamodb
import sys

from boundary_aws_plugin.cloudwatch_plugin import CloudwatchPlugin
from boundary_aws_plugin.cloudwatch_metrics import CloudwatchMetrics


class DynamoDBCloudwatchMetrics(CloudwatchMetrics):
    def __init__(self, access_key_id, secret_access_key):
        return super(DynamoDBCloudwatchMetrics, self).__init__(access_key_id, secret_access_key, 'AWS/DynamoDB')

    def get_region_list(self):
        # Some regions are returned that actually do not support DynamoDB.  Skip those.
        return [r for r in boto.dynamodb.regions() if r.name not in ['cn-north-1', 'us-gov-west-1']]

    def get_entities_for_region(self, region):
        ddb = boto.connect_dynamodb(self.access_key_id, self.secret_access_key, region=region)
        return ddb.list_tables()

    def get_entity_source_name(self, table_name):
        return table_name

    def get_entity_dimensions(self, region, table_name):
        return dict(TableName=table_name)

    def get_metric_list(self):
        return (
            ('SuccessfulRequestLatency', 'Average', 'AWS_DYNAMODB_SUCCESSFUL_REQUEST_LATENCY'),
            ('UserErrors', 'Average', 'AWS_DYNAMODB_USER_ERRORS'),
            ('SystemErrors', 'Average', 'AWS_DYNAMODB_SYSTEM_ERRORS'),
            ('ThrottledRequests', 'Sum', 'AWS_DYNAMODB_THROTTLED_REQUESTS'),
            ('ReadThrottleEvents', 'Sum', 'AWS_DYNAMODB_READ_THROTTLE_EVENTS'),
            ('WriteThrottleEvents', 'Sum', 'AWS_DYNAMODB_WRITE_THROTTLE_EVENTS'),
            ('ProvisionedReadCapacityUnits', 'Average', 'AWS_DYNAMODB_PROVISIONED_READ_CAPACITY_UNITS'),
            ('ProvisionedWriteCapacityUnits', 'Average', 'AWS_DYNAMODB_PROVISIONED_WRITE_CAPACITY_UNITS'),
            ('ConsumedReadCapacityUnits', 'Average', 'AWS_DYNAMODB_CONSUMED_READ_CAPACITY_UNITS'),
            ('ConsumedWriteCapacityUnits', 'Average', 'AWS_DYNAMODB_CONSUMED_WRITE_CAPACITY_UNITS'),
            ('ReturnedItemCount', 'Average', 'AWS_DYNAMODB_RETURNED_ITEM_COUNT'),
        )


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '-v':
        import logging
        logging.basicConfig(level=logging.INFO)

    plugin = CloudwatchPlugin(DynamoDBCloudwatchMetrics, 'NEM_', 'boundary-plugin-aws-dynamodb-python-status')
    plugin.main()

