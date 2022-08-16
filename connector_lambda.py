import json
import paramiko
import boto3
from io import StringIO
import os
import base64
from botocore.exceptions import ClientError




def lambda_handler(event, context):
    #1.     ########################################################### Secret maneger / UserName & password session sftp #####################################################

        client = boto3.client('secretsmanager')
        
        try:
            response = client.get_secret_value(
                SecretId='## path_to_secret_manager ##'
            )
            database_secrets = json.loads(response['SecretString'])
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in response:
                hostname = (database_secrets['hostname'])
                username = (database_secrets['username'])
                password = (database_secrets['password'])
            else:
                decoded_binary_secret = base64.b64decode(response['SecretBinary'])

    #2.     ########################################################### S3 bucket source PATH (output of Elastic-subs-project) ###########################################  
        args = {
                'bucket_name':'*name_of_bucket',
                'output_folder':'*path',
                'name':'*File_Name'
                }
    
        bucket_name = args['bucket_name']
        
        dwn = boto3.client('s3')
    
        s3_resource = boto3.resource('s3')
        my_bucket = s3_resource.Bucket(args['bucket_name'])
        
        ## files -> bucket s3 source object contain .csv in filename 
        files = [file.key for file in my_bucket.objects.filter(Prefix=args['output_folder']) if args['name'] in file.key]
        ## remote_path -> destination to send the object from s3 bucket 
        remote_path = 'data/'
        
            #####       Test if bucket s3 is empty ...    ############
        if bool(files):
            for file in files:
                ################################### VARIABLES temp  #############################
                file_name = file.split('/')[-1]
                download_path = '/tmp/{}'.format(file_name)
                # upload_path = '/tmp/{}'.format(file_name.split('.')[0])
                
        
        
                dwn.download_file('bucket_name', file, download_path)
                upload_key = file_name
                    
                            ##################   download from s3 and Upload to same s3 for verify the content of 'download_path'  #############
                    # dwn.upload_file(download_path, bucket_name, upload_key)
                    
    ##3.     ######################################################### Connexion Sftp with ssh client & paramiko methods    ########################################
        #         ssh=paramiko.SSHClient()
        #         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #         ssh.connect(hostname=hostname, username=username, password=password)
        #         sftp_client=ssh.open_sftp()
                
        #         stdin, stdout, stderr = ssh.exec_command('ls -l')
        #         if stdout.channel.recv_exit_status() != 1:
        #             print(f'error with the exit status from the process on the server, code:{stdout.channel.recv_exit_status()}')
        #             # error handling, your command was not succesful
        #             ssh.close()         
                
        #         else:
        #         # #passing localpath, remotepath
        #             sftp_client.put(localpath=download_path, remotepath=os.path.join(remote_path,upload_key))

                    
        #             ## fermeture connexion sftp & ssh 
        #             sftp_client.close()
        #             ssh.close()
        # else:
        #     print('no file\'s name *name  in bucket S3 ')
        

        return("* the file is correctly push on the sftp destination")
