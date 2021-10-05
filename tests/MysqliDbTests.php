<?php

use Fraeio\Database\MysqliDb;
use PHPUnit\Framework\TestCase;

class MysqliDbTests extends TestCase {

  private MysqliDb $db;
  private string $prefix;

  public function setUp(): void {
    $this->db = $this->DatabaseConnectionMethods();
    $this->prefix = 'PHPUnitTesting_';
    $this->db->setPrefix($this->prefix);
  }

  private function DatabaseConnectionMethods(): MysqliDb {
    if (empty($_ENV['DB_HOST'])) {
      self::fail('Must provide database to test with');
    }
    $host = $_ENV['DB_HOST'];
    $username = $_ENV['DB_USER'];
    $password = $_ENV['DB_PASS'];
    $dbname = $_ENV['DB_NAME'];

    $MANUAL_DB = new MysqliDb($host, $username, $password, $dbname);
    self::assertIsObject($MANUAL_DB);

    $ENVIRONMENT_DB = new MysqliDb();
    self::assertIsObject($ENVIRONMENT_DB);

    $ARRAY_DB = new MysqliDb([
      'host' => $host,
      'username' => $username,
      'password' => $password,
      'dbname' => $dbname
    ]);
    self::assertIsObject($ARRAY_DB);

    $OBJECT_DB = new MysqliDb(new mysqli($host, $username, $password, $dbname));
    self::assertIsObject($OBJECT_DB);

    return $OBJECT_DB;
  }

  public function testPrefixQuery(): void {
    self::assertEquals($this->db->prefix, $this->prefix);
  }

  public function testCreateTable_and_InsertData_query(): void {
    if ($this->db->has('users')) {
      $this->db->drop("users");
    }
    if ($this->db->has('products')) {
      $this->db->drop("products");
    }
    if ($this->db->has('test')) {
      $this->db->drop("test");
    }

    $insert_data = [
      'users' => [
        [
          'login' => 'user1',
          'customerId' => 10,
          'firstName' => 'John',
          'lastName' => 'Doe',
          'password' => $this->db->func('SHA1(?)', ["secretpassword+salt"]),
          'createdAt' => $this->db->now(),
          'expires' => $this->db->now('+1Y'),
          'loginCount' => $this->db->inc()
        ],
        [
          'login' => 'user2',
          'customerId' => 10,
          'firstName' => 'Mike',
          'lastName' => NULL,
          'password' => $this->db->func('SHA1(?)', ["secretpassword2+salt"]),
          'createdAt' => $this->db->now(),
          'expires' => $this->db->now('+1Y'),
          'loginCount' => $this->db->inc(2)
        ],
        [
          'login' => 'user3',
          'active' => TRUE,
          'customerId' => 11,
          'firstName' => 'Pete',
          'lastName' => 'D',
          'password' => $this->db->func('SHA1(?)', ["secretpassword2+salt"]),
          'createdAt' => $this->db->now(),
          'expires' => $this->db->now('+1Y'),
          'loginCount' => $this->db->inc(3)
        ]
      ],
      'products' => [
        [
          'customerId' => 11,
          'productName' => 'product1',
        ],
        [
          'customerId' => 11,
          'productName' => 'product2',
        ],
        [
          'customerId' => 11,
          'productName' => 'product3',
        ],
        [
          'customerId' => 10,
          'productName' => 'product4',
        ],
        [
          'customerId' => 10,
          'productName' => 'product5',
        ]
      ]
    ];
    $bad_data = [
      'users' => [
        [
          'login' => NULL,
          'customerId' => 10,
          'firstName' => 'John',
          'lastName' => 'Doe',
          'password' => 'test',
          'createdAt' => $this->db->now(),
          'expires' => $this->db->now('+1Y'),
          'loginCount' => $this->db->inc()
        ]
      ]
    ];
    $this->insertTable([
      'users' => [
        'login' => 'CHAR(10) NOT NULL',
        'active' => 'BOOL DEFAULT 0',
        'customerId' => 'INT(10) NOT NULL',
        'firstName' => 'CHAR(10) NOT NULL',
        'lastName' => 'CHAR(10)',
        'password' => 'TEXT NOT NULL',
        'createdAt' => 'DATETIME',
        'expires' => 'DATETIME',
        'loginCount' => 'INT(10) DEFAULT 0'
      ],
      'products' => [
        'customerId' => 'INT(10) NOT NULL',
        'productName' => 'CHAR(50)'
      ]
    ]);
    $this->insertData($insert_data);
    $this->insertData($bad_data, TRUE);
  }

  public function testInsertData_AutoIncrement(): void {
    $q = "create table {$this->prefix}test (id int(10), name varchar(10));";
    $this->db->prepare($q);
    $id = $this->db->insert("test", [
      "id" => 1,
      "name" => "testname"
    ]);
    self::assertNotEmpty($id, 'insert without autoincrement failed');
    $this->db->get("test");
    self::assertEquals(1, $this->db->count, 'insert without autoincrement failed -- wrong insert count');

    $q = "drop table {$this->prefix}test;";
    $this->db->prepare($q);
  }

  public function testInsertData_helpers(): void {
    //Order users by ASC
    $this->db->orderBy("`id`", "ASC");
    $this->db->get("users");
    self::assertEquals(3, $this->db->count, 'Invalid total insert count');

    //sql injection test
    $this->db->where('id', "' or 1=1; drop table PHPUnitTesting_users; --");
    $this->db->get("users");
    $users = $this->db->get("users");
    self::assertNotEmpty($users, 'Users is empty, SQL INJECTION');

    //Order users by ASC with custom fields
    $this->db->orderBy("login", "ASC", [
      "user3",
      "user2",
      "user1"
    ]);
    $login = $this->db->getValue("users", "login");
    self::assertEquals('user3', $login, 'Order by field test failed');

    //count active users (expects 1) with method chaining
    $active_users = $this->db->where("active", TRUE)->get('users');
    self::assertCount(1, $active_users, 'Invalid total get count with boolean, from method chaining');
    self::assertEquals(1, $this->db->count, 'Invalid total insert count with boolean');

    //change in-active users to active.
    $this->db->where("active", 0);
    $this->db->update("users", ["active" => 1]);
    self::assertEquals(2, $this->db->count, 'Invalid update count with active users changed to active');

    //recount active users (expects 3)
    $this->db->where("active", TRUE);
    $this->db->get("users");
    self::assertEquals(3, $this->db->count, 'Invalid total insert count with boolean');

    //count active users with number rows 2 (expects 2)
    $this->db->where("active", TRUE);
    $this->db->get("users", 2);
    self::assertEquals(2, $this->db->count, 'Invalid total insert count with boolean with num rows');

    //Interval (expects 3)
    $this->db->where("createdAt", [">" => $this->db->interval("-1h")]);
    $this->db->get("users");
    self::assertEquals(3, $this->db->count, 'Interval (createdAt over 1 hour ago)');

    //find users with firstname like 'John' (expects 1)
    $this->db->where("firstname", ['LIKE' => '%John%']);
    $this->db->get("users");
    self::assertEquals(1, $this->db->count, 'Invalid insert count in LIKE');

    //group customers and count
    $this->db->groupBy("customerId");
    $this->db->get("users", NULL, 'customerId, count(id) as cnt');
    self::assertEquals(2, $this->db->count, 'Invalid records count with group by');

    //update data (expects 1)
    $upData = [
      'expires' => $this->db->now("+5M", "expires"),
      'loginCount' => $this->db->inc()
    ];
    $this->db->where("login", 'user1');
    $this->db->update("users", $upData);
    self::assertEquals(1, $this->db->count, 'Invalid update count with functions');

    //find user with login user1 (expects 1, and valid password)
    $this->db->where("login", 'user1');
    $user = $this->db->getOne("users");
    self::assertTrue(isset($user));
    self::assertEquals(1, $this->db->count, 'Invalid users count on getOne()');
    self::assertEquals('546f98b24edfdc3b9bbe0d241bd8b29783f71b32', $user['password'], 'Invalid password were set');

    //find users with customerId 10 or 11 (expects 3)
    $this->db->where("customerId", [
      '10',
      '11'
    ], 'IN');
    $this->db->get("users");
    self::assertEquals(3, $this->db->count, 'Invalid users count on where() with (IN)');

    //find users with id between 1 and 100 (expects 3)
    //NOTE needs to be 100 because of Galera Clustering and how they handle ID's
    $this->db->where("id", [
      '1',
      '100'
    ], 'between');
    $this->db->get("users");
    self::assertEquals(3, $this->db->count, 'Invalid users count on where() with (between)');

    //find users with id 1 or customerId 11 (expects 2)
    $this->db->where("firstName", 'John');
    $this->db->orWhere("customerId", 11);
    $this->db->get("users");
    self::assertEquals(2, $this->db->count, 'Invalid users count on orWhere()');

    //find users with lastName NULL (expects 1)
    $this->db->where("lastName", NULL, '<=>');
    $this->db->get("users");
    self::assertEquals(1, $this->db->count, 'Invalid users count on null where()');

    //join user.id and products.userId tables. (expects 3)
    $this->db->join("users u", "p.customerId=u.customerId", "LEFT");
    $this->db->where("u.login", 'user3');
    $this->db->orderBy("CONCAT(u.login, u.firstName)");
    $this->db->get("products p", NULL, "u.login, p.productName");
    self::assertEquals(3, $this->db->count, 'Invalid products count on join ()');

    //find users with bindParams as array (expects 2)
    $this->db->where("login = ? or customerId = ?", [
      'user1',
      11
    ]);
    $this->db->get("users");
    self::assertEquals(2, $this->db->count, 'Invalid users count on select with multiple params');

    //find users with bindParams (expects 2)
    $this->db->where("login = 'user1' or customerId = 11");
    $this->db->get("users");
    self::assertEquals(2, $this->db->count, 'Invalid users count on select with multiple params');

    //delete and drop tables
    $this->db->delete("users");
    $this->db->get("users");
    self::assertEquals(0, $this->db->count, 'Invalid users count after delete');
    $this->db->delete("products");
    $this->db->drop("users");
    $this->db->drop("products");

    //trace output
    print_r($this->db->trace);
  }

  /** @noinspection PhpSameParameterValueInspection */
  private function insertTable($tables): void {
    foreach ($tables as $name => $fields) {
      //$this->db->prepare("DROP TABLE ".$this->prefix.$name);
      $this->db->createTable($name, $fields);
      self::assertFalse($this->db->has($name));
    }
  }

  private function insertData($data, $bad_data = FALSE): void {
    foreach ($data as $name => $items) {
      foreach ($items as $d) {
        $id = $this->db->insert($name, $d);
        if ($id) {
          $d['id'] = $id;
          if ($bad_data) {
            self::fail("bad data insert failed!");
          }
        } elseif (!$bad_data) {
          self::fail("failed to insert: " . $this->db->getLastQuery() . "\n" . $this->db->getLastError());
        }
      }
    }
  }

}