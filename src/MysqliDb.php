<?php
/** @noinspection PhpUnused */

namespace fraeio\database;

use JetBrains\PhpStorm\ArrayShape;
use JetBrains\PhpStorm\Pure;
use mysqli as MySQL;
use mysqli_stmt;
use RuntimeException;

/**
 * MySQLi Model for representing a connection between PHP and MySQL.
 *
 * @category Database Access
 * @package  Fraeio\Database
 * @version  0.1.0
 *
 */
class MysqliDb {

  protected static MysqliDb $_instance;
  public array $trace = [];

  /* --------------- *
     Query Variables
   * --------------- */
  public null|string $prefix = '';
  public int $count = 0;
  public int $totalCount = 0;
  protected MySQL $_mysqli;
  protected null|string $_query;
  protected null|string $_lastQuery;
  protected array $_queryOptions = [];
  protected array $_join = [];
  protected array $_where = [];

  /* ----------------- *
     Execution Tracing
   * ----------------- */
  protected array $_orderBy = [];
  protected array $_groupBy = [];
  protected array $_bindParams = [''];
  protected null|string $_stmtError;
  protected bool $_transaction_in_progress = FALSE;

  /* --------------- *
     Other Variables
   * --------------- */
  protected int $traceStartQ;
  protected bool $traceEnabled = TRUE;
  protected null|string $traceStripPrefix = '';

  /* --------------------- *
      Database Credentials
   * --------------------- */
  protected string $host;
  protected null|string $username;
  protected null|string $password;
  protected null|string $dbname;
  protected null|string $port;
  protected string $charset;

  /**
   * Open a new connection to a MySQL server
   * @param array|MySQL|string|null $host     [optional] Hostname or IP address of MySQL server, Passing the NULL value
   *                                          will connect using environment DB_HOST variable. You can also specify an
   *                                          existing database connection by passing an MySQL object.
   * @param string|null             $username [optional] Username for authentication, Passing the NULL value
   *                                          will connect using environment DB_USER variable.
   * @param string|null             $password [optional]Password for authentication, Passing the NULL value will
   *                                          connect using environment DB_PASS variable.
   * @param string|null             $dbname   [optional] If provided, this value will specify the database to be used
   *                                          when performing queries. Passing the NULL value will connect using
   *                                          environment DB_NAME variable.
   * @param int                     $port     [optional] specifies the port number to connect to the server, Passing
   *                                          the NULL value will connect using environment DB_PORT variable.
   * @param string                  $charset  [optional] Specify the character set, Passing no value will connect
   *                                          using utf8.
   * @param string|null             $prefix   [optional] Specifies the prefix for database table names, Passing the
   *                                          NULL value will use the environment DB_PREFIX variable.
   */
  public function __construct(string|array|MySQL $host = NULL, string $username = NULL, string $password = NULL, string $dbname = NULL, int $port = 3306, string $charset = 'utf8', string $prefix = NULL) {
    $user = $pass = $name = NULL;
    self::$_instance = $this;

    //MySQLi object
    if (is_object($host)) {
      $this->_mysqli = $host;
    } //Array
    elseif (is_array($host)) {
      foreach ($host as $key => $value) {
        $$key = $value;
      }
    } //Environment Variables
    elseif (isset ($_ENV['DB_HOST'])) {
      $host = $_ENV['DB_HOST'];
      $username = (string)$_ENV['DB_USER'];
      $password = (string)$_ENV['DB_PASS'];
      $dbname = (string)$_ENV['DB_NAME'];
      $port = (int)$_ENV['DB_PORT'];
      $charset = (string)$_ENV['DB_CHARSET'];
      $prefix = (string)$_ENV['DB_PREFIX'];
    }

    //Save Configuration
    if (is_string($host) && !empty($host)) {
      $this->host = $host;
      $this->username = $username ?? $user;
      $this->password = $password ?? $pass;
      $this->dbname = $dbname ?? $name;
      $this->port = $port;
      $this->charset = $charset;
      $this->prefix = $prefix;
      //throw if host is empty OR not string/object.
    } elseif (!is_object($host)) {
      throw new RuntimeException('No Database Params Set on _construct');
    }

    if (!is_object($host)) {
      $this->connect();
    }
  }

  private function connect(): void {
    if (empty($this->host)) {
      throw new RuntimeException('Database info not set Error');
    }
    //mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $this->_mysqli = new MySQL($this->host, $this->username, $this->password, $this->dbname, $this->port);
    if ($this->_mysqli->connect_error) {
      throw new RuntimeException('Database Connection Error ' . $this->_mysqli->connect_error);
    }
    /* Set the desired charset after establishing a connection */
    $this->_mysqli->set_charset($this->charset);
    if ($this->_mysqli->errno) {
      throw new RuntimeException('mysqli error: ' . $this->_mysqli->error);
    }
  }

  /**
   * Return static instance to allow access to the object from within another class.
   * Inheriting this class would require reloading connection info.
   *
   * This is a very sensitive method.
   *
   * @return MysqliDb
   * @uses Database::getInstance();
   */
  public static function getInstance(): MysqliDb {
    return self::$_instance;
  }

  /**
   * Set a prefix for table names for queries.
   *
   * @param string|null $prefix
   */
  public function setPrefix(string $prefix = NULL): void {
    $this->prefix = $prefix;
  }

  /**
   * Performs a query on the database.
   *
   * @param string         $query   Contains a user-provided select query.
   * @param array|int|null $numRows Array to define SQL limits in format Array ($count, $offset)
   *
   * @return array|MysqliDb|null Contains the returned rows from the query.
   */
  public function query(string $query, int|array|null $numRows = NULL): array|MysqliDb|null {
    $this->_query = filter_var($query, FILTER_SANITIZE_STRING);
    $stmt = $this->_buildQuery($numRows);

    $stmt->execute();
    $this->_stmtError = $stmt->error;
    $res = $this->_dynamicBindResults($stmt);
    $this->reset();
    return $res;
  }

  /**
   * Abstraction method that will compile the WHERE statement,
   * any passed update data, and the desired rows.
   * It then builds the SQL query.
   *
   * @param array|int|null $numRows   Array to define SQL limits in format Array ($count, $offset)
   *                                  or only $count
   * @param array|null     $tableData Should contain an array of data for updating the database.
   *
   * @return mysqli_stmt Returns the $stmt object.
   */
  protected function _buildQuery(int|array $numRows = NULL, array $tableData = NULL): mysqli_stmt {
    $this->_buildJoin();
    $this->_buildTableData($tableData);
    $this->_buildWhere();
    $this->_buildGroupBy();
    $this->_buildOrderBy();
    $this->_buildLimit($numRows);

    $this->_lastQuery = $this->replacePlaceHolders($this->_query, $this->_bindParams);

    // Prepare query
    $stmt = $this->_prepareQuery();

    // Bind parameters to statement if any
    if (count($this->_bindParams) > 1) {
      call_user_func_array([
        $stmt,
        'bind_param'
      ], $this->refValues($this->_bindParams));
    }

    return $stmt;
  }

  /**
   * Abstraction method that will build an JOIN part of the query
   */
  protected function _buildJoin(): void {
    if (empty ($this->_join)) {
      return;
    }

    foreach ($this->_join as $data) {
      [
        $joinType,
        $joinTable,
        $joinCondition
      ] = $data;

      if (is_object($joinTable)) {
        $joinStr = $this->_buildPair('', $joinTable);
      } else {
        $joinStr = $joinTable;
      }

      $this->_query .= " " . $joinType . " JOIN " . $joinStr . " on " . $joinCondition;
    }
  }

  /**
   * Helper function to add variables into bind parameters array and will return
   * its SQL part of the query according to operator in ' $operator ? '
   *
   * @param               $operator
   * @param string        $value
   * @return string
   */
  protected function _buildPair($operator, string $value): string {
    $this->_bindParam($value);
    return ' ' . $operator . ' ? ';
  }

  /**
   * Helper function to add variables into bind parameters array
   *
   * @param string|null Variable value
   */
  protected function _bindParam(string|null $value): void {
    $this->_bindParams[0] .= $this->_determineType($value);
    $this->_bindParams[] = $value;
  }

  /**
   * This method is needed for prepared statements. They require
   * the data type of the field to be bound with "i" "s", etc.
   * This function takes the input, determines what type it is,
   * and then updates the param_type.
   *
   * @param mixed $item Input to determine the type.
   *
   * @return string The joined parameter types.
   */
  protected function _determineType(mixed $item): string {
    if (is_array($item) || is_object($item) || is_resource($item)) {
      throw new RuntimeException("Unsupported Type");
    }
    //todo handle BLOB types. blob => b
    return match (gettype($item)) {
      'NULL', 'string' => 's',
      'boolean', 'integer' => 'i',
      'double' => 'd',
      default => '',
    };
  }

  /**
   * Abstraction method that will build an INSERT or UPDATE part of the query
   * @param $tableData
   */
  protected function _buildTableData($tableData): void {
    if (!is_array($tableData)) {
      return;
    }

    $isInsert = strpos($this->_query, 'INSERT');
    $isUpdate = strpos($this->_query, 'UPDATE');

    if ($isInsert !== FALSE) {
      $this->_query .= ' (`' . implode('`, `', array_keys($tableData)) . '`)';
      $this->_query .= ' VALUES (';
    } else {
      $this->_query .= " SET ";
    }

    foreach ($tableData as $column => $value) {
      if ($isUpdate !== FALSE) {
        $this->_query .= "`" . $column . "` = ";
      }

      // Sub query value
      if (is_object($value)) {
        $this->_query .= $this->_buildPair("", $value) . ", ";
        continue;
      }

      // Simple value
      if (!is_array($value)) {
        $this->_bindParam($value);
        $this->_query .= '?, ';
        continue;
      }

      // Function value
      $key = key($value);
      $val = $value[$key];
      switch ($key) {
        case '[I]':
          $this->_query .= $column . $val . ", ";
          break;
        case '[F]':
          $this->_query .= $val[0] . ", ";
          if (!empty ($val[1])) {
            $this->_bindParams($val[1]);
          }
          break;
        case '[N]':
          if ($val === NULL) {
            $this->_query .= "!" . $column . ", ";
          } else {
            $this->_query .= "!" . $val . ", ";
          }
          break;
        default:
          throw new RuntimeException("Wrong operation");
      }
    }
    $this->_query = rtrim($this->_query, ', ');
    if ($isInsert !== FALSE) {
      $this->_query .= ')';
    }
  }

  /**
   * Helper function to add variables into bind parameters array in bulk
   *
   * @param array Variable with values
   */
  protected function _bindParams(array $values): void {
    foreach ($values as $value) {
      $this->_bindParam($value);
    }
  }

  /**
   * Abstraction method that will build the part of the WHERE conditions
   */
  protected function _buildWhere(): void {
    if (empty ($this->_where)) {
      return;
    }

    //Prepare the where portion of the query
    $this->_query .= ' WHERE';

    foreach ($this->_where as $cond) {
      [
        $concat,
        $varName,
        $operator,
        $val
      ] = $cond;
      $this->_query .= " " . $concat . " " . $varName;

      switch (strtolower($operator)) {
        case 'not in':
        case 'in':
          $comparison = ' ' . $operator . ' (';
          if (is_object($val)) {
            $comparison .= $this->_buildPair("", $val);
          } else {
            foreach ($val as $v) {
              $comparison .= ' ?,';
              $this->_bindParam($v);
            }
          }
          $this->_query .= rtrim($comparison, ',') . ' ) ';
          break;
        case 'not between':
        case 'between':
          $this->_query .= " $operator ? and ? ";
          $this->_bindParams($val);
          break;
        case 'not exists':
        case 'exists':
          $this->_query .= $operator . $this->_buildPair("", $val);
          break;
        default:
          if (is_array($val)) {
            $this->_bindParams($val);
          } elseif ($val === NULL) {
            $this->_query .= $operator . " NULL";
          } elseif ($val !== 'DBNULL') {
            $this->_query .= $this->_buildPair($operator, $val);
          }
      }
    }
  }

  /**
   * Abstraction method that will build the GROUP BY part of the WHERE statement
   */
  protected function _buildGroupBy(): void {
    if (empty ($this->_groupBy)) {
      return;
    }

    $this->_query .= " GROUP BY ";
    foreach ($this->_groupBy as $value) {
      $this->_query .= $value . ", ";
    }

    $this->_query = rtrim($this->_query, ', ') . " ";
  }

  /**
   * Abstraction method that will build the LIMIT part of the WHERE statement
   */
  protected function _buildOrderBy(): void {
    if (empty ($this->_orderBy)) {
      return;
    }

    $this->_query .= " ORDER BY ";
    foreach ($this->_orderBy as $prop => $value) {
      if (strtolower(str_replace(" ", "", $prop)) === 'rand()') {
        $this->_query .= "rand(), ";
      } else {
        $this->_query .= $prop . " " . $value . ", ";
      }
    }

    $this->_query = rtrim($this->_query, ', ') . " ";
  }

  /**
   * Abstraction method that will build the LIMIT part of the WHERE statement
   *
   * @param int|array|null $numRows Array to define SQL limits in format Array ($count, $offset)
   *                                or only $count
   */
  protected function _buildLimit(int|array|null $numRows): void {
    if (!isset ($numRows)) {
      return;
    }

    if (is_array($numRows)) {
      $this->_query .= ' LIMIT ' . (int)$numRows[0] . ', ' . (int)$numRows[1];
    } else {
      $this->_query .= ' LIMIT ' . (int)$numRows;
    }
  }

  /**
   * Function to replace ? with variables from bind variable
   * @param string $str
   * @param array  $vals
   *
   * @return string
   */
  #[Pure]
  protected function replacePlaceHolders(string $str, array $vals): string {
    $i = 1;
    $newStr = "";

    while ($pos = strpos($str, " ? ")) {
      $val = $vals[$i++];
      if (is_object($val)) {
        $val = '[object]';
      }
      if ($val === NULL) {
        $val = 'NULL';
      }
      $newStr .= substr($str, 0, $pos) . "'" . $val . "'";
      $str = substr($str, $pos + 1);
    }
    $newStr .= $str;
    return $newStr;
  }

  /**
   * Method attempts to prepare the SQL query
   * and throws an error if there was a problem.
   *
   * @return mysqli_stmt
   */
  protected function _prepareQuery(): mysqli_stmt {
    if (!$stmt = $this->_mysqli->prepare($this->_query)) {
      throw new RuntimeException("Unable to prepare MySQL statement, check your syntax($this->_query) " . $this->_mysqli->error);
    }
    if ($this->traceEnabled) {
      $this->traceStartQ = microtime(TRUE);
    }

    return $stmt;
  }

  /**
   * @param array $arr
   *
   * @return array
   */
  #[Pure]
  protected function refValues(array $arr): array {
    //Reference is required for PHP 5.3+
    if (strnatcmp(PHP_VERSION, '5.3') >= 0) {
      $refs = [];
      foreach ($arr as $key => $value) {
        /** @noinspection PhpArrayAccessCanBeReplacedWithForeachValueInspection */
        $refs[$key] = &$arr[$key];
      }
      return $refs;
    }
    return $arr;
  }

  /**
   * This helper method takes care of prepared statements' "bind_result method
   * , when the number of variables to pass is unknown.
   *
   * @param mysqli_stmt $stmt Equal to the prepared statement object.
   *
   * @return array The results of the SQL fetch.
   */
  protected function _dynamicBindResults(mysqli_stmt $stmt): array {
    $parameters = [];
    $results = [];

    $meta = $stmt->result_metadata();

    // if $meta is false yet sqlstate is true, there's no sql error but the query is
    // most likely an update/insert/delete which doesn't produce any results
    if (!$meta && $stmt->sqlstate) {
      return [];
    }

    $row = [];
    while ($field = $meta->fetch_field()) {
      $row[$field->name] = NULL;
      $parameters[] = &$row[$field->name];
    }

    call_user_func_array([
      $stmt,
      'bind_result'
    ], $parameters);

    $this->totalCount = 0;
    $this->count = 0;
    while ($stmt->fetch()) {
      $x = [];
      foreach ($row as $key => $val) {
        $x[$key] = $val;
      }
      $this->count++;
      $results[] = $x;
    }
    // stored procedures sometimes can return more than 1 result set
    if ($this->_mysqli->more_results()) {
      $this->_mysqli->next_result();
    }

    if (in_array('SQL_CALC_FOUND_ROWS', $this->_queryOptions, TRUE)) {
      /** @noinspection CallableParameterUseCaseInTypeContextInspection */
      $stmt = $this->_mysqli->query('SELECT FOUND_ROWS()');
      $totalCount = $stmt->fetch_row();
      $this->totalCount = $totalCount[0];
    }

    return $results;
  }

  /**
   * Reset all building variables and tracing.
   */
  protected function reset(): void {
    if ($this->traceEnabled) {
      $this->trace[] = [
        $this->_lastQuery,
        (microtime(TRUE) - $this->traceStartQ),
        $this->_traceGetCaller()
      ];
    }

    $this->_where = [];
    $this->_join = [];
    $this->_orderBy = [];
    $this->_groupBy = [];
    $this->_bindParams = [''];
    $this->_query = NULL;
    $this->_queryOptions = [];
  }

  /**
   * Get where and what function was called for query stored in MysqliDB->trace
   *
   * @return string with information
   */
  private function _traceGetCaller(): string {
    $dd = debug_backtrace();
    $caller = next($dd);
    while (isset ($caller) && $caller["file"] === __FILE__) {
      $caller = next($dd);
    }

    return __CLASS__ . "->" . $caller["function"] . "() >> file \"" . str_replace($this->traceStripPrefix, '', $caller["file"]) . "\" line #" . $caller["line"] . " ";
  }

  /**
   * Function to enable SQL_CALC_FOUND_ROWS in the get queries
   *
   * @return MysqliDb
   */
  public function withTotalCount(): MysqliDb {
    $this->setQueryOption('SQL_CALC_FOUND_ROWS');
    return $this;
  }

  /**
   * This method allows you to specify multiple options for SQL queries.
   * supports method chaining.
   *
   * @param array|string $options The options name of the query.
   *
   * @return MysqliDb
   * @example Database->setQueryOption($options)->setQueryOption($options);
   *
   */
  public function setQueryOption(array|string $options): MysqliDb {
    $allowedOptions = [
      'ALL',
      'DISTINCT',
      'DISTINCTROW',
      'HIGH_PRIORITY',
      'STRAIGHT_JOIN',
      'SQL_SMALL_RESULT',
      'SQL_BIG_RESULT',
      'SQL_BUFFER_RESULT',
      'SQL_CACHE',
      'SQL_NO_CACHE',
      'SQL_CALC_FOUND_ROWS',
      'LOW_PRIORITY',
      'IGNORE',
      'QUICK'
    ];
    if (!is_array($options)) {
      $options = [$options];
    }

    foreach ($options as $option) {
      $option = strtoupper($option);
      if (!in_array($option, $allowedOptions)) {
        throw new RuntimeException('Wrong query option: ' . $option);
      }

      $this->_queryOptions[] = $option;
    }

    return $this;
  }

  /* ----------------- *
     Helper Functions
   * ----------------- */

  /**
   * A convenient SELECT * function to get one value.
   *
   * @param string $tableName The name of the database table to work with.
   * @param        $column
   *
   * @return string|array|null Contains the returned column from the select query.
   */
  public function getValue(string $tableName, $column): string|array|null {
    $res = $this->get($tableName, 1, "$column as retval");
    return $res[0]["retval"] ?? NULL;
  }

  /**
   * A convenient SELECT * function.
   *
   * @param string         $tableName The name of the database table to work with.
   * @param array|int|null $numRows   Array to define SQL limits in format Array ($count, $offset) or only $count
   * @param string|array   $columns
   *
   *
   * @return array|MysqliDb|null Contains the returned rows from the select query.
   */
  public function get(string $tableName, array|int $numRows = NULL, string|array $columns = '*'): array|MysqliDb|null {
    if (empty ($columns)) {
      $columns = '*';
    }

    $column = is_array($columns) ? implode(', ', $columns) : $columns;
    $this->_query = 'SELECT ' . implode(' ', $this->_queryOptions) . ' ' . $column . " FROM " . $this->prefix . $tableName;
    $stmt = $this->_buildQuery($numRows);

    $stmt->execute();
    $this->_stmtError = $stmt->error;
    $res = $this->_dynamicBindResults($stmt);
    $this->reset();

    return $res;
  }

  /**
   * A convenient INSERT function.
   *
   * @param string $tableName  The name of the table.
   * @param array  $insertData Data containing information for inserting into the DB.
   *
   * @return bool|MysqliDb|null indicating whether the insert query was completed successfully.
   */
  public function insert(string $tableName, array $insertData): bool|null|MysqliDb {

    $this->_query = "INSERT INTO " . $this->prefix . $tableName;
    $stmt = $this->_buildQuery(NULL, $insertData);

    $stmt->execute();
    $this->_stmtError = $stmt->error;
    $this->reset();
    $this->count = $stmt->affected_rows;

    if ($stmt->affected_rows < 1) {
      return FALSE;
    }

    if ($stmt->insert_id > 0) {
      return $stmt->insert_id;
    }
    return TRUE;
  }

  /**
   * A convenient function that returns TRUE if exists at least an element that
   * satisfy the where condition specified calling the "where" method before this one.
   *
   * @param string $tableName The name of the database table to work with.
   *
   * @return bool Contains the number of rows matching the condition specified.
   */
  public function has(string $tableName): bool {
    try {
      $this->getOne($tableName, '1');
    } catch (RuntimeException) {
      return FALSE;
    }
    return $this->count >= 1;
  }

  /**
   * A convenient SELECT * function to get one record.
   *
   * @param string $tableName The name of the database table to work with.
   * @param string $columns
   *
   * @return MysqliDb|array|null Contains the returned row from the select query.
   */
  public function getOne(string $tableName, string $columns = '*'): MysqliDb|array|null {
    $res = $this->get($tableName, 1, $columns);

    if (is_object($res)) {
      return $res;
    }

    return $res[0] ?? NULL;
  }

  /**
   * Update query. Be sure to first call the "where" method.
   *
   * @param string $tableName The name of the database table to work with.
   * @param array  $tableData Array of data to update the desired row.
   *
   * @return bool|MysqliDb|null
   */
  public function update(string $tableName, array $tableData): bool|null|MysqliDb {
    $this->_query = "UPDATE " . $this->prefix . $tableName;
    $stmt = $this->_buildQuery(NULL, $tableData);

    $status = $stmt->execute();
    $this->reset();
    $this->_stmtError = $stmt->error;
    $this->count = $stmt->affected_rows;

    return $status;
  }

  /**
   * Delete query. Call the "where" method first.
   *
   * @param string         $tableName The name of the database table to work with.
   * @param array|int|null $numRows   Array to define SQL limits in format Array ($count, $offset)
   *                                  or only $count
   *
   * @return bool|MysqliDb|null Indicates success. 0 or 1.
   */
  public function delete(string $tableName, int|array $numRows = NULL): bool|null|MysqliDb {
    /** @noinspection SqlWithoutWhere */
    $this->_query = "DELETE FROM " . $this->prefix . $tableName;
    $stmt = $this->_buildQuery($numRows);

    $stmt->execute();
    $this->_stmtError = $stmt->error;
    $this->reset();

    return ($stmt->affected_rows > 0);
  }

  /**
   * Drop query. Call the "where" method first.
   *
   * @param string $tableName The name of the database table to work with.
   * @return bool|MysqliDb|null Indicates success. 0 or 1.
   */
  public function drop(string $tableName): bool|null|MysqliDb {
    $this->_query = "DROP TABLE " . $this->prefix . $tableName;
    $stmt = $this->_buildQuery();

    $stmt->execute();
    $this->_stmtError = $stmt->error;
    $this->reset();

    return ($stmt->affected_rows > 0);
  }

  /**
   * This method allows you to specify "WHERE" statement for SQL query.
   * [optional] you can specify multiple "OR WHERE" statements if you method chain.
   *
   * @param string $whereProp  The name of the database field.
   * @param mixed  $whereValue The value of the database field.
   * @param string $operator
   *
   * @return MysqliDb
   * @example Database->orWhere('id', 7)->orWhere('title', 'MyTitle');
   *
   */
  public function orWhere(string $whereProp, string|int $whereValue = 'DBNULL', string $operator = '='): MysqliDb {
    return $this->where($whereProp, $whereValue, $operator, 'OR');
  }

  /**
   * This method allows you to specify "WHERE" statement for SQL query.
   * [optional] you can specify multiple "AND WHERE" statements if you method chain.
   *
   * @param string $whereProp  The name of the database field
   * @param mixed  $whereValue The value of the database field.
   * @param string $operator
   * @param string $cond
   *
   * @return MysqliDb
   * @example Database->where('id', 7)->where('title', 'MyTitle');
   *
   */
  public function where(string $whereProp, string|int|array|null $whereValue = 'DBNULL', string $operator = '=', string $cond = 'AND'): MysqliDb {
    /** @noinspection TypeUnsafeComparisonInspection */
    if (is_array($whereValue) && ($key = key($whereValue)) != "0") {
      /** @noinspection CallableParameterUseCaseInTypeContextInspection */
      $operator = $key;
      $whereValue = $whereValue[$key];
    }
    if (count($this->_where) === 0) {
      $cond = '';
    }
    $this->_where[] = [
      $cond,
      $whereProp,
      $operator,
      $whereValue
    ];
    return $this;
  }

  /**
   * This method allows you to concatenate joins for the final SQL statement.
   *
   * @param string|object $joinTable     The name of the table.
   * @param string        $joinCondition the condition.
   * @param string        $joinType      'LEFT', 'INNER' etc.
   *
   * @return MysqliDb
   *
   */
  public function join(string|object $joinTable, string $joinCondition, string $joinType = ''): MysqliDb {
    $allowedTypes = [
      'LEFT',
      'RIGHT',
      'OUTER',
      'INNER',
      'LEFT OUTER',
      'RIGHT OUTER'
    ];
    $joinType = strtoupper(trim($joinType));

    if ($joinType && !in_array($joinType, $allowedTypes)) {
      throw new RuntimeException('Wrong JOIN type: ' . $joinType);
    }

    if (!is_object($joinTable)) {
      $joinTable = $this->prefix . filter_var($joinTable, FILTER_SANITIZE_STRING);
    }

    $this->_join[] = [
      $joinType,
      $joinTable,
      $joinCondition
    ];

    return $this;
  }

  /**
   * This method allows you to specify "ORDER BY" statements for SQL queries.
   * [optional] you can specify multiple "AND ORDER BY" statements if you method chain.
   *
   * @param string            $orderByField The name of the database field.
   * @param string            $orderByDirection
   * @param string|array|null $customFields
   *
   * @return MysqliDb
   * @example Database->orderBy('id', 'desc')->orderBy('name', 'desc');
   */
  public function orderBy(string $orderByField, string $orderByDirection = "DESC", string|array $customFields = NULL): MysqliDb {
    $allowedDirection = [
      "ASC",
      "DESC"
    ];
    $orderByDirection = strtoupper(trim($orderByDirection));
    $orderByField = preg_replace("/[^-a-z0-9.(),_`]+/i", '', $orderByField);

    // Add table prefix to orderByField if needed.
    //FIXME: We are adding prefix only if table is enclosed into `` to distinguish aliases from table names
    $orderByField = preg_replace('/(`)([`a-zA-Z0-9_]*\.)/', '\1' . $this->prefix . '\2', $orderByField);


    if (empty($orderByDirection) || !in_array($orderByDirection, $allowedDirection)) {
      throw new RuntimeException('Wrong order direction: ' . $orderByDirection);
    }

    if (is_array($customFields)) {
      foreach ($customFields as $key => $value) {
        $customFields[$key] = preg_replace("/[^-a-z0-9.(),_`]+/i", '', $value);
      }
      $orderByField = 'FIELD (' . $orderByField . ', "' . implode('","', $customFields) . '")';
    }

    $this->_orderBy[$orderByField] = $orderByDirection;
    return $this;
  }

  /**
   * This method allows you to specify "GROUP BY" statements for SQL queries.
   * [optional] you can specify multiple "AND GROUP BY" statements if you method chain.
   *
   * @param string $groupByField The name of the database field.
   *
   * @return MysqliDb
   * @example Database->groupBy($groupByField);
   */
  public function groupBy(string $groupByField): MysqliDb {
    $groupByField = preg_replace("/[^-a-z0-9.(),_]+/i", '', $groupByField);

    $this->_groupBy[] = $groupByField;
    return $this;
  }

  /**
   * This method will return the ID of the last inserted item
   *
   * @return int The last inserted item ID.
   */
  public function getInsertId(): int {
    return $this->_mysqli->insert_id;
  }

  /**
   * Escape harmful characters which might affect a query.
   *
   * @param string $str The string to escape.
   *
   * @return string The escaped string.
   */
  public function escape(string $str): string {
    return $this->_mysqli->real_escape_string($str);
  }

  /**
   * Method to call mysqli->ping() to keep unused connections open on
   * long-running scripts, or to reconnect timed out connections (if php.ini has
   * global mysqli. Reconnect set to true). Can't do this directly using object
   * since _mysqli is protected.
   *
   * @return bool True if connection is up
   */
  public function ping(): bool {
    return $this->_mysqli->ping();
  }

  /**
   * Close connection
   */
  public function __destruct() {
    $this->_mysqli?->close();
  }

  /**
   * Close connection
   * @return bool True if connection is closed
   */
  public function close(): bool {
    return $this->_mysqli?->close();
  }

  /**
   * Method returns generated interval function as an insert/update function
   *
   * @param string|null interval in the formats:
   *        "1", " - 1d" or " - 1 day" -- For interval - 1 day
   *        Supported intervals [s]second, [m]minute, [h]hour, [d]day, [M]month, [Y]year
   *        Default null;
   * @param string Initial date
   *
   * @return array
   */
  #[ArrayShape(["[F]" => "string[]"])]
  public function now(string|null $diff = NULL, string $func = "NOW()"): array {
    return ["[F]" => [$this->interval($diff, $func)]];
  }

  /**
   * Method returns generated interval function as a string
   *
   * @param string|null $diff interval in the formats:
   *                          "1", " - 1d" or " - 1 day" -- For interval - 1 day
   *                          Supported intervals [s]second, [m]minute, [h]hour, [d]day, [M]month, [Y]year
   *                          Default null;
   * @param string      $func Initial date
   *
   * @return string
   */
  public function interval(string|null $diff, string $func = "NOW()"): string {
    $types = [
      "s" => "second",
      "m" => "minute",
      "h" => "hour",
      "d" => "day",
      "M" => "month",
      "Y" => "year"
    ];
    $incr = '+';
    $items = '';
    $type = 'd';

    if ($diff && preg_match('/([+-]?) ?([\d]+) ?([a-zA-Z]?)/', $diff, $matches)) {
      if (!empty ($matches[1])) {
        $incr = $matches[1];
      }
      if (!empty ($matches[2])) {
        $items = $matches[2];
      }
      if (!empty ($matches[3])) {
        $type = $matches[3];
      }
      if (!array_key_exists($type, $types)) {
        throw new RuntimeException("invalid interval type in '$diff'");
      }
      $func .= " " . $incr . " interval " . $items . " " . $types[$type] . " ";
    }
    return $func;
  }

  /**
   * Method generates incremental function call
   * @param int increment amount. 1 by default
   * @return array
   */
  #[ArrayShape(["[I]" => "string"])]
  public function inc($num = 1): array {
    return ["[I]" => " + " . (int)$num];
  }

  /**
   * Method generates detrimental function call
   * @param int increment amount. 1 by default
   * @return array
   */
  #[ArrayShape(["[I]" => "string"])]
  public function dec($num = 1): array {
    return ["[I]" => " - " . (int)$num];
  }

  /**
   * Method generates change boolean function call
   * @param null|string $col
   * @return array
   */
  #[ArrayShape(["[N]" => "string"])]
  public function not(string $col = NULL): array {
    return ["[N]" => (string)$col];
  }

  /**
   * Method generates user defined function call
   * @param string $expr user function body
   * @param null   $bindParams
   * @return array
   */
  #[ArrayShape([
    "[F]" => "array
      "
  ])]
  public function func(string $expr, $bindParams = NULL): array {
    return [
      "[F]" => [
        $expr,
        $bindParams
      ]
    ];
  }

  /* -------------- *
      Transactions
   * -------------- */

  /**
   * Begin a transaction
   *
   * @uses    mysqli->autocommit()
   * @uses    register_shutdown_function()
   */
  public function startTransaction(): void {
    $this->_mysqli->autocommit(FALSE);
    $this->_transaction_in_progress = TRUE;
    register_shutdown_function([
      $this,
      "_transaction_status_check"
    ]);
  }

  /**
   * Transaction commit
   *
   * @uses mysqli->commit();
   * @uses mysqli->autocommit(true);
   */
  public function commit(): void {
    $this->_mysqli->commit();
    $this->_transaction_in_progress = FALSE;
    $this->_mysqli->autocommit(TRUE);
  }

  /**
   * Shutdown handler to rollback uncommitted operations in order to keep
   * atomic operations sane.
   *
   * @uses mysqli->rollback();
   */
  public function _transaction_status_check(): void {
    if (!$this->_transaction_in_progress) {
      return;
    }
    $this->rollback();
  }

  /**
   * Transaction rollback function
   *
   * @uses mysqli->rollback();
   * @uses mysqli->autocommit(true);
   */
  public function rollback(): void {
    $this->_mysqli->rollback();
    $this->_transaction_in_progress = FALSE;
    $this->_mysqli->autocommit(TRUE);
  }

  /* ---------------- *
          Trace
   * ---------------- */

  /**
   * Query execution time tracking switch
   *
   * @param bool        $enabled     Enable execution time tracking
   * @param null|string $stripPrefix Prefix to strip from the path in exec log
   * @return MysqliDb
   */
  public function setTrace(bool $enabled, string $stripPrefix = NULL): MysqliDb {
    $this->traceEnabled = $enabled;
    $this->traceStripPrefix = $stripPrefix;
    return $this;
  }

  /**
   * Method returns last executed query
   *
   * @return string|null
   */
  public function getLastQuery(): ?string {
    return $this->_lastQuery;
  }



  /* -------------- *
     get Functions
   * -------------- */

  /**
   * Method returns mysql error
   *
   * @return string
   */
  #[Pure]
  public function getLastError(): string {
    return trim($this->_stmtError . " " . $this->_mysqli->error);
  }

  /**
   * Change database
   *
   * @param string $database
   * @return bool
   */
  public function selectDatabase(string $database): bool {
    return $this->_mysqli->select_db($database);
  }

  /* ---------------- *
      More Functions
   * ---------------- */

  public function createTable(string $name, array $fields): array {
    $q = "CREATE TABLE " . $this->prefix . $name . " (id INT(9) UNSIGNED PRIMARY KEY AUTO_INCREMENT";
    foreach ($fields as $key => $value) {
      $q .= ", $key $value";
    }
    $q .= ")";
    return $this->prepare($q);
  }

  /**
   * Pass a query and an array containing the parameters to bind to the prepared statement for execution.
   *
   * @param string     $query      Contains a user-provided query.
   * @param array|null $bindParams All variables to bind to the SQL statement.
   * @param bool       $sanitize   If query should be filtered before execution
   *
   * @return array Contains the returned rows from the query.
   */
  public function prepare(string $query, array $bindParams = NULL, bool $sanitize = TRUE): array {
    $params = [''];
    $this->_query = $query;
    if ($sanitize) {
      $this->_query = filter_var($query, FILTER_SANITIZE_STRING, FILTER_FLAG_NO_ENCODE_QUOTES);
    }
    $stmt = $this->_prepareQuery();

    if (is_array($bindParams) === TRUE) {
      foreach ($bindParams as $val) {
        $params[0] .= $this->_determineType($val);
        $params[] = $val;
      }
      call_user_func_array([
        $stmt,
        'bind_param'
      ], $this->refValues($params));
    }

    $stmt->execute();
    if ($stmt->error) {
      throw new RuntimeException("Database Execution Failed ($stmt->errno): " . $stmt->error);
    }
    $this->_stmtError = $stmt->error;
    $this->_lastQuery = $this->replacePlaceHolders($this->_query, $params);
    $res = $this->_dynamicBindResults($stmt);
    $this->reset();

    return $res;
  }
}