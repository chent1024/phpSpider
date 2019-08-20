<?php
use XCrawler\Utils;

// 默认配置
return [
    // 日志配置
    'log' => [
        // 日志文件路径
        'path' => 'log/spider-'.date('Y-m-d').'.log',
    ],

    // redis配置
    'redis' => [
        'prefix' => null,
        'host' => '127.0.0.1',
        'password' => null,
        'port' => 6379,
        'database' => 0,
    ],
    // db config
    'database' => [
        'database_type' => 'mysql',
        'database_name' => 'news',
        'server' => 'localhost',
        'port' => 3306,
        'username' => 'root',
        'password' => 'root',
        'prefix' => '',
        'charset' => 'utf8mb4',
        'collation' => 'utf8mb4_general_ci',
        'logging' => true,
    ]
];
