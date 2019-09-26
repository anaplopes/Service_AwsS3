# -*- coding: utf-8 -*-
import os
import logging
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError


class AwsS3Service:

    def __init__(self, bucket_name, object_name, diretory_object=None):
        """
            :param bucket_name: nome do bucket
            :param object_name: nome do arquivo
            :param diretory_object: nome da pasta dentro bucket. Se não informado será atribuido o bucket.
        """

        self.s3_client = boto3.client('s3',
                                      region_name=os.getenv('region_name'),
                                      aws_access_key_id=os.getenv('aws_access_key_id'),
                                      aws_secret_access_key=os.getenv('aws_secret_access_key'))
        self.bucket_name = bucket_name
        self._object_name = object_name
        self.diretory_object = diretory_object

    @property
    def object_name(self):
        return self._object_name

    @object_name.setter
    def object_name(self, value):
        self._object_name = value

    @object_name.getter
    def object_name(self):
        if self.diretory_object:
            self._object_name = f'{self.diretory_object}/{self._object_name}'
        return self._object_name

    def upload_file_s3(self, local_file):
        """ Carregar arquivo para o S3

            :param local_file: local do arquivo
            :return: True para sucesso, se não False
        """

        try:
            self.s3_client.upload_file(local_file, self.bucket_name, self.object_name)
            return True
        except ClientError as ecli:
            logging.error(ecli)
            return False
        except FileNotFoundError as efile:
            logging.error(efile)
            return False
        except NoCredentialsError as ecred:
            logging.error(ecred)
            return False
        except Exception as e:
            logging.critical(e)
            return False

    def download_file_s3(self, local_file):
        """ Baixar arquivo do S3

            :param local_file: local do arquivo
            :return: True para sucesso, se não False
        """

        local_name_file = f'{local_file}/{self._object_name}'

        try:
            self.s3_client.download_file(self.bucket_name, self.object_name, local_name_file)
            return True
        except ClientError as ecli:
            if ecli.response['Error']['Code'] == "404":
                logging.error(ecli)
                print("The object does not exist.")
            else:
                logging.error(ecli)
            return False
        except Exception as e:
            logging.critical(e)
            return False

    def create_url_s3(self, expiration):
        """ Gerar URL S3 para compartilhamento

            :param expiration: tempo em segundos de validade da URL, se não informado será atribuido 7 dias
            :return: String url para sucesso, se não None
        """

        try:
            response = self.s3_client.generate_presigned_url('get_object',
                                                             Params={'Bucket': self.bucket_name, 'Key': self.object_name},
                                                             ExpiresIn=expiration)
            return response
        except ClientError as ecli:
            logging.error(ecli)
            return None
        except Exception as e:
            logging.critical(e)
            return None
