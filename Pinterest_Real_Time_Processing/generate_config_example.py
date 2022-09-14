import configparser

if __name__ == '__main__':
    # CREATE OBJECT
    config_file = configparser.ConfigParser()

    # ADD SECTION
    config_file.add_section("pgAdminAuth")
    # ADD SETTINGS TO SECTION
    config_file.set("pgAdminAuth", "rdb_name", "xxxxxx")
    config_file.set("pgAdminAuth", "username", "xxxxxx")
    config_file.set("pgAdminAuth", "password", "xxxxxx")
    config_file.set("pgAdminAuth", "host", "xxxxxx")
    config_file.set("pgAdminAuth", "port", "xxxx")
    config_file.set("pgAdminAuth", "table", "xxxxxx")

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