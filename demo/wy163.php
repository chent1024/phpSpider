<?php
date_default_timezone_set('PRC');
require '../vendor/autoload.php';
use PhpSpider\Spider;

const BASE_URI = 'https://3g.163.com/touch/reconstruct/article/list/BBM54PGAwangning/__PAGE__-10.html';
//$DB = db();

$request = function () {
    for ($page = 0; $page <= 100; $page += 10) {
        $uri = str_replace('__PAGE__', $page, BASE_URI);
//        echo $uri.PHP_EOL;
        yield $uri;
    }
};

$success = function ($result, $request, $spider, $header){
//    global $DB;
    if (empty($result)) {
        return;
    }

    echo $request['uri'];
    echo PHP_EOL;
    $posts = jsonp_decode($result);
    if (!$posts) {
        return;
    }
//    print_r($posts);
//
//    foreach ($posts['BBM54PGAwangning'] as $val) {
//        $inserted = $DB->has('post', ['url' => $val['url']]);
//        if ($inserted) {
//            continue;
//        }
//
//        $data = [
//            'title' => $val['title'],
//            'description' => $val['digest'],
//            'auth' => $val['source'],
//            'src' => '网易新闻',
//            'url' => $val['url'],
//            'cover' => $val['imgsrc'],
//            'comment' => $val['commentCount'],
//            'post_at' => $val['ptime'],
//        ];
//        $DB->insert('post', $data);
//    }
};

(new Spider([
    'name' => '163:3g',
    'interval' => 0,
    'concurrency' => 3,
    'requests' => $request,
    'success' => $success,
    'error' => function ($request, $error, $rs) {
        echo $error;
    }
]))->run();

