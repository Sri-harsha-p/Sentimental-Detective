import boto3
import os
import json
import uuid
import time

s3 = boto3.client('s3', 'us-east-1')
dynamodb = boto3.client('dynamodb', 'us-east-1')
sns = boto3.client('sns', 'us-east-1')
bedrock = boto3.client('bedrock-runtime', 'us-east-1')

TABLE_NAME = os.environ['TABLE_NAME']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
MODEL_ID = os.environ['MODEL_ID']

def lambda_handler(event, context):
    try:
   
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print(f"Processing file from S3: s3://{bucket}/{key}")


        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read())

        review_text = data.get("reviewText", "No review text provided.")
        

        prompt = f"""
        Analyze this customer review:
        \"{review_text}\"

        Respond in JSON format with keys:
        - sentiment (positive/neutral/negative)
        - key_topic
        - urgency_level (low/medium/high)
        """
        
     
        bedrock_response = bedrock.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 512,
                "temperature": 0.5,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }),
            contentType="application/json",
            accept="application/json"
        )

        raw_output = bedrock_response['body'].read().decode()
        print("Raw response from Bedrock:", raw_output)

        result = json.loads(raw_output)

        analysis_text = result['content'][0]['text']

        try:
    
            if '```json' in analysis_text:
                json_start = analysis_text.find('{')
                json_end = analysis_text.rfind('}') + 1
                analysis_json = analysis_text[json_start:json_end]
            else:
                
                json_start = analysis_text.find('{')
                json_end = analysis_text.rfind('}') + 1
                if json_start != -1 and json_end != 0:
                    analysis_json = analysis_text[json_start:json_end]
                else:
                    analysis_json = analysis_text
            
            analysis_data = json.loads(analysis_json)
        except Exception as parse_error:
            print(f"Error parsing JSON: {parse_error}")
            print(f"Raw analysis text: {analysis_text}")
            # Fallback analysis
            analysis_data = {
                "sentiment": "unknown",
                "key_topic": "unknown",
                "urgency_level": "low"
            }

        
        item = {
            'reviewerID': {'S': data.get('reviewerID', f'reviewer_{str(uuid.uuid4())[:8]}')},
            'review_id': {'S': str(uuid.uuid4())},
            'asin': {'S': data.get('asin', 'unknown')},
            'sentiment': {'S': analysis_data['sentiment']},
            'key_topic': {'S': analysis_data['key_topic']},
            'urgency_level': {'S': analysis_data['urgency_level']},
            'reviewText': {'S': review_text},
            'ttl': {'N': str(int(time.time()) + 86400)}
        }

        
        dynamodb.put_item(TableName=TABLE_NAME, Item=item)

        
        if analysis_data['sentiment'].lower() == "negative":
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="Negative Review Alert",
                Message=f"Review: {review_text}\n\nAnalysis: {json.dumps(analysis_data, indent=2)}"
            )

        return {
            'statusCode': 200,
            'body': json.dumps('Processed review successfully.')
        }

    except Exception as e:
        print("Error processing event:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }