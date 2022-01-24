import platform
import os


def read_in_aws_credentials():

    cred_file_path = "./credentials"

    cred_file = open(cred_file_path, 'r')
    file_lines = cred_file.readlines()
    access_key = file_lines[1].split(" ", 2)[2].strip()
    secret_access_key = file_lines[2].split(" ", 2)[2].strip()
    return (access_key, secret_access_key)


if __name__ == '__main__':
    creds = read_in_aws_credentials()   
    print(creds[1])
    print(creds[0])