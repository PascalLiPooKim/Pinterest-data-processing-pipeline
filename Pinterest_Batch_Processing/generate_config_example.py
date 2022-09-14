import configparser

if __name__ == '__main__':
    # CREATE OBJECT
    config_file = configparser.ConfigParser()

    # ADD SECTION
    config_file.add_section("default")
    # ADD SETTINGS TO SECTION
    config_file.set("default", "kafka_topic", 'xxxxxxxxx')
    config_file.set("default", "temp_json_file", "xxxxxxxxx.json")
    config_file.set("default", "aws_s3_bucket", "xxxxxxxxx")
    config_file.set("default", "pin_data_s3_dir", "xxxxxxxxx")

    # SAVE CONFIG FILE
    with open(r"configurations.ini", 'w') as configfileObj:
        config_file.write(configfileObj)
        configfileObj.flush()
        configfileObj.close()

    print("Config file 'configurations.ini' created")

    # PRINT FILE CONTENT
    read_file = open("configurations.ini", "r")
    content = read_file.read()
    print("Content of the config file are:\n")
    print(content)
    read_file.flush()
    read_file.close()