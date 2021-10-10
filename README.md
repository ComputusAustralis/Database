
# FraeCMS Database
This package provides a simplified model for PHP MySQLi (and potentially more in the future). Intended for FraeCMS for added simplicity and security.
## Installation
### Composer
`composer require fraeio/database`

### Requirements
For MySQLi functions to be available, you must enable MySQLi extension to work with your PHP instances.  

> For installation instructions for MySQLi, go to: [PHP.net MySQLi
> Installation](http://php.net/manual/en/mysqli.installation.php
> "http://php.net/manual/en/mysqli.installation.php")

### Configuration
By default, FraeCMS Database will use environment variables to attempt to connect to a database server. In your .ENV file you can specify these variables:

    DB_HOST = localhost # Specify hostname or TCP/IP of MySQL server to connect to.
    DB_USER = admin # The MySQL user name.
    DB_PASS = password # The MySQL password. [optionaly] provide null.
    DB_NAME = FraeCMS # Specify the default database to be used when performing queries.
    DB_PORT = 3306 # [optional] Specify MySQL port.
    DB_CHARSET = utf8 # [optional] Set the desired charset for the database connection.
    DB_PREFIX = frae_ # [optional] Prefix for database table names.`

## Create a connection
### Example #1 Connect using environment variables

    /* Load Environment Variables. */
	    Dotenv::createImmutable(CONFIG_PATH, '.env')->load();
    
    /* Connect to Database using environment variables */
	    $database = new Database();

### Example #2 Connect using ini or other variables

    /* Connect to Database using ini variables */
	    $database = new Database(
	      ini_get('mysqli.default_host'),
	      ini_get('mysqli.default_user'),
	      ini_get('mysqli.default_pw'),
	      'my_database',
	      ini_get('mysqli.default_port'),
	      'utf8',
	      'optional_table_prefix_',
	    );

### Example #3 Connect using an array

    /* Connect to Database using array variables */
        $database = new Database([
          'host': 'localhost',
          'username': 'my_user',
          'password': 'my_password',
          'dbname': 'my_database',
          'port': 3306,
          'charset': 'utf8',
          'prefix': NULL,
        ]);

