import json
import urllib.parse
import boto3
import csv
print('Loading function')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    if "json" in key:
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            response_body = response['Body'].read()
            data = json.loads(response_body.decode('utf-8'))
            Flat_json = flatten_json(data['web-app']) 
            write_csv(Flat_json,'|')
            upload_file()
            #write the data into '/tmp' folder
            return response['ContentType']
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e
        
def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a],name +a+ '|')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '|')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out
    
# Upload the file
def upload_file():
    #Method 2: Client.put_object()
    s3 = boto3.resource('s3')
    #try:
        #print('bucketname'+str(bucket))
    s3.Bucket('testingdmsunique').upload_file(r'/tmp/SampleJson.csv', 'SampleJson1.csv')
    #except ClientError as e:
    #    logging.error(type(e))
    #    return False
    return True

def write_csv(flat_json,delim):
    data_file = open(r'/tmp/SampleJson.csv', 'w',newline='') 
    # create the csv writer object 
    csv_writer = csv.writer(data_file) 
    # Counter variable used for writing  
    # headers to the CSV file 
    count = 0
    count2 = 0
    print(data_file)
    
    for f in flat_json: 
        if count == 0: 
            # Writing headers of CSV file 
            header = flat_json.keys()
            csv_writer.writerow(header) 
            count += 1  
    # Writing data of CSV file 
    for k,v in flat_json.items():
        concat = str(k)+str(delim)+str(v)
        csv_writer.writerow([concat])
    data_file.close()
    
        
